#include "fkv.h"
#include "tools/xxhash.h"
#include "flow_controler.h"
#include "zipf.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>

#include <sched.h>
#include <pthread.h>
#include <thread>
#include <vector>
#include <csignal>

#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_byteorder.h>
#include <rte_ip.h>
#include <rte_ether.h>

#define SET_THREAD_NUM 10
#define GET_THREAD_NUM 18
#define MAX_CHECKER 36
#define MAX_THREAD_NUM 72

/* Following protocol speicific parameters should be same with MEGA */
#define MEGA_PKT_END 0xffff
#define MEGA_JOB_GET 0x2
#define MEGA_JOB_SET 0x3
// #define MEGA_JOB_DEL 0x4 // TODO: DEL is to be implemented
#define MEGA_END_MARK_LEN 2U
#define PROTOCOL_TYPE_LEN 2U
#define PROTOCOL_KEYLEN_LEN 2U
#define PROTOCOL_VALLEN_LEN 4U
#define PROTOCOL_HEADER_LEN 8U

#define GET_RESPOND_LEN 2U
#define GET_MAX_RETURN_LEN 16U
#define SET_RESPOND_LEN 2U
#define SET_MAX_RETURN_LEN 2U

#define RESPOND_ALL_COUNTERS 8U
#define SET_SUCC 0x13
#define SET_FAIL 0x23
#define GET_SUCC 0x12
#define GET_FAIL 0x22

#define NUM_QUEUE NUM_THREAD
#define RX_RING_SIZE 1024
#define TX_RING_SIZE 1024

#define NUM_MBUFS 8191
#define MBUF_CACHE_SIZE 250

#define BURST_SIZE (4U)
#define PKG_GEN_COUNT 1

// static struct rte_ether_addr S_Addr = {{0x98, 0x03, 0x9b, 0x8f, 0xb0, 0x11}};//{0x98, 0x03, 0x9b, 0x8f, 0xb1, 0xc9}
// static struct rte_ether_addr D_Addr = {{0x98, 0x03, 0x9b, 0x8f, 0xb5, 0xc0}};//{0x04, 0x3f, 0x72, 0xdc, 0x26, 0x24}

static struct rte_ether_addr S_Addr = {{0x98, 0x03, 0x9b, 0x8f, 0xb1, 0xc9}};
static struct rte_ether_addr D_Addr = {{0x04, 0x3f, 0x72, 0xdc, 0x26, 0x25}};

// #define IP_SRC_ADDR ((192U << 24) | (168U << 16) | (1U << 8) | 101U)
// #define IP_DST_ADDR ((192U << 24) | (168U << 16) | (1U << 8) | 103U)

#define IP_SRC_ADDR ((10U << 24) | (176U << 16) | (64U << 8) | 41U)
#define IP_DST_ADDR ((10U << 24) | (176U << 16) | (64U << 8) | 36U)


// 14(ethernet header) + 20(IP header) + 8(UDP header)
#define EIU_HEADER_LEN 42
#define ETHERNET_HEADER_LEN 14
#define ETHERNET_MAX_FRAME_LEN 1514
#define ETHERNET_MIN_FRAME_LEN 64
// IP pkt header
#define IP_DEFTTL 64 /* from RFC 1340. */
#define IP_VERSION 0x40
#define IP_HDRLEN 0x05 /* default IP header length == five 32-bits words. */
#define IP_VHL_DEF (IP_VERSION | IP_HDRLEN)

struct rte_mempool *recv_mbuf_pool[NUM_QUEUE];
struct rte_mempool *send_mbuf_pool;
#define NUM_MAX_CORE 36
struct benchmark_core_statistics {
  uint64_t tx;
  uint64_t rx;
  uint64_t dropped;
  uint64_t err_ending;
  int enable;
} __rte_cache_aligned;
struct benchmark_core_statistics core_statistics[NUM_MAX_CORE];

typedef struct context_s {
  unsigned int core_id;
  unsigned int queue_id;
} context_t;

size_t set_core_affinity = 1;

DataStore datastore;
hash_table mytable;

const size_t page_size = 1048576 * 2;  // page size = 2 MiB
const size_t num_pages_to_try = 240;   // SHM_MAX_PAGES;

std::vector<std::thread> workers;

#ifdef _DUMP_PKT
FILE *fp[NUM_MAX_CORE];

extern "C" void show_pkt(struct rte_mbuf *pkt) {
  int pktlen = pkt->data_len - EIU_HEADER_LEN;
  uint8_t *ptr = (uint8_t *)((uint8_t *)rte_pktmbuf_mtod(pkt, uint8_t *) + EIU_HEADER_LEN);
  // fprintf(fp[sched_getcpu()], "pkt_len: %d\n", pktlen);
  while (*(uint16_t *)ptr != 0xFFFF) {
    uint32_t key_len = *(uint16_t *)(ptr + PROTOCOL_TYPE_LEN);
    if (*(uint16_t *)ptr == MEGA_JOB_GET) {
      fprintf(fp[sched_getcpu()], "GET\t%lu\n", *(uint64_t *)(ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN));
      ptr += PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN + key_len;
    } else if (*(uint16_t *)ptr == MEGA_JOB_SET) {
      uint32_t val_len = *(uint16_t *)(ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN);
      fprintf(fp[sched_getcpu()], "SET\t%lu\t%lu\n", *(uint64_t *)(ptr + PROTOCOL_HEADER_LEN),
              *(uint64_t *)(ptr + PROTOCOL_HEADER_LEN + key_len));
      ptr += PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN + PROTOCOL_VALLEN_LEN + key_len + val_len;
    }
    // fprintf(fp[sched_getcpu()], "\n");
  }
  fprintf(fp[sched_getcpu()], "END_MARK: %04x \n", *ptr);
  fprintf(fp[sched_getcpu()], "\n");
  fflush(fp[sched_getcpu()]);
}
#endif

extern "C" void show_pkt(struct rte_mbuf *pkt) {
  // int pktlen = pkt->data_len - EIU_HEADER_LEN;
  uint8_t *ptr = (uint8_t *)((uint8_t *)rte_pktmbuf_mtod(pkt, uint8_t *) + EIU_HEADER_LEN);
  while (*(uint16_t *)ptr != 0xFFFF) {
    uint32_t key_len = *(uint16_t *)(ptr + PROTOCOL_TYPE_LEN);
    if (*(uint16_t *)ptr == MEGA_JOB_GET) {
      fprintf(stdout, "GET\t%lu\n", *(uint64_t *)(ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN));
      ptr += PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN + key_len;
    } else if (*(uint16_t *)ptr == MEGA_JOB_SET) {
      uint32_t val_len = *(uint16_t *)(ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN);
      fprintf(stdout, "SET\t%lu\t%lu\n", *(uint64_t *)(ptr + PROTOCOL_HEADER_LEN),
              *(uint64_t *)(ptr + PROTOCOL_HEADER_LEN + key_len));
      ptr += PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN + PROTOCOL_VALLEN_LEN + key_len + val_len;
    }
  }
  fprintf(stdout, "END_MARK: %04x \n", *ptr);
  fprintf(stdout, "\n");
  fflush(stdout);
}

extern "C" void print_stats();

extern "C" void port_init();

/* This is for `rte_pktmbuf_mtod1 can be used in gdb. */
extern "C" uint8_t *mtod(struct rte_mbuf *mbuf) { return (uint8_t *)rte_pktmbuf_mtod(mbuf, uint8_t *); }

extern "C" inline void show_mac(const char *info, struct rte_ether_addr *addr);

extern "C" void show_system_status(const hash_table *table);

extern "C" void set_mt(hash_table *table, SlabStore *store, size_t t_id) {
  int core_id = t_id;
  if (set_core_affinity) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(core_id, &mask);
    if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0)
      fprintf(stderr, "[Error] set thread affinity failed\n");
  }
  core_statistics[core_id].enable = 1;

  uint8_t *ptr = NULL;
  uint16_t i, port = 0;
  struct rte_ether_hdr *ethh;
  struct rte_ipv4_hdr *ip_hdr;
  struct rte_udp_hdr *udph;

  struct rte_mbuf *tx_bufs_pt[PKG_GEN_COUNT];
  struct rte_mbuf *rx_buf[BURST_SIZE];

  for (int i = 0; i < PKG_GEN_COUNT; i++) {
    struct rte_mbuf *pkt = (struct rte_mbuf *)rte_pktmbuf_alloc((struct rte_mempool *)send_mbuf_pool);
    if (pkt == NULL) rte_exit(EXIT_FAILURE, "Cannot alloc storage memory in  port %" PRIu16 "\n", port);
    // pkt->data_len = pktlen;
    pkt->nb_segs = 1;  // nb_segs
    // pkt->pkt_len = pkt->data_len;
    pkt->ol_flags = PKT_TX_IPV4;  // ol_flags
    pkt->vlan_tci = 0;            // vlan_tci
    pkt->vlan_tci_outer = 0;      // vlan_tci_outer
    pkt->l2_len = sizeof(struct rte_ether_hdr);
    pkt->l3_len = sizeof(struct rte_ipv4_hdr);

    ethh = (struct rte_ether_hdr *)rte_pktmbuf_mtod(pkt, unsigned char *);
    ethh->s_addr = S_Addr;
    ethh->d_addr = D_Addr;
    ethh->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

    ip_hdr = (struct rte_ipv4_hdr *)((unsigned char *)ethh + sizeof(struct rte_ether_hdr));
    ip_hdr->version_ihl = IP_VHL_DEF;
    ip_hdr->type_of_service = 0;
    ip_hdr->fragment_offset = 0;
    ip_hdr->time_to_live = IP_DEFTTL;
    ip_hdr->next_proto_id = IPPROTO_UDP;
    // ip_hdr->next_proto_id = IPPROTO_IP;
    ip_hdr->packet_id = 0;
    // ip_hdr->total_length = rte_cpu_to_be_16(pktlen);
    ip_hdr->src_addr = rte_cpu_to_be_32(IP_SRC_ADDR);
    ip_hdr->dst_addr = rte_cpu_to_be_32(IP_DST_ADDR);
    // ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);

    udph = (struct rte_udp_hdr *)((unsigned char *)ip_hdr + sizeof(struct rte_ipv4_hdr));
    // udph->src_port = 123;
    // udph->dst_port = 123;
    // udph->dgram_len = rte_cpu_to_be_16((uint16_t)(pktlen - sizeof(struct rte_ether_hdr)
    // 													 -
    // sizeof(struct rte_ipv4_hdr))); udph->dgram_cksum = 0;

    tx_bufs_pt[i] = pkt;
  }

  bool ret;
  uint64_t key_hash;
  uint32_t key_len, val_len;
  uint16_t nb_rx, nb_tx;
  uint16_t get_succ = 0, set_succ = 0, get_fail = 0, set_fail = 0;
  int pktlen = EIU_HEADER_LEN, pkt_id = 0;
  uint8_t *tx_ptr = (uint8_t *)rte_pktmbuf_mtod(tx_bufs_pt[pkt_id], uint8_t *) + EIU_HEADER_LEN;
  while (table->is_running) {
    nb_rx = rte_eth_rx_burst(port, t_id, rx_buf, BURST_SIZE);

    core_statistics[core_id].rx += nb_rx;

    /* test TX/RX; */
    /* if (nb_rx != 0) 
    printf("nb_rx:%d\n",nb_rx); */

      auto check_pkt_end = [&](struct rte_mbuf *pkt) {
        int pkt_len = pkt->data_len;
        uint16_t *ptr = (uint16_t *)((uint8_t *)rte_pktmbuf_mtod(pkt, uint8_t *) + (pkt_len - 2));
        assert(*ptr == MEGA_PKT_END);
        /* Verbosely check content of response pkts.
        uint16_t key_len;
        uint32_t val_len;
        char *w_ptr = (char *)rte_pktmbuf_mtod(pkt, char *) + EIU_HEADER_LEN;
        while (*(uint16_t *)w_ptr != MEGA_PKT_END) {
          if (*(uint16_t *)w_ptr == GET_SUCC) {
            key_len = *(uint16_t *)(w_ptr + PROTOCOL_TYPE_LEN);
            val_len = *(uint32_t *)(w_ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN);
            w_ptr += PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN + PROTOCOL_VALLEN_LEN + key_len + val_len;
          } else if (*(uint16_t *)w_ptr == GET_FAIL) {
            w_ptr += SET_RESPOND_LEN;
          } else if (*(uint16_t *)w_ptr == SET_SUCC) {
            w_ptr += SET_RESPOND_LEN;
          } else if (*(uint16_t *)w_ptr == SET_FAIL) {
            w_ptr += SET_RESPOND_LEN;
          } else {
            // break;
            assert(0);
          }
        } */
      };
      auto complement_pkt = [&](struct rte_mbuf *pkt, uint8_t *ptr, int pktlen) {
        uint16_t *counter = (uint16_t *)((uint8_t *)rte_pktmbuf_mtod(pkt, uint8_t *) + EIU_HEADER_LEN);
        //bwb: ↑ ↑ ↑ 不使用传进来的参数指针，而是重新定位data头指针赋值counter
        *counter = get_succ;
        get_succ = 0;
        counter += 1;
        *counter = set_succ;
        set_succ = 0;
        counter += 1;
        *counter = get_fail;
        get_fail = 0;
        counter += 1;
        *counter = set_fail;
        set_fail = 0;

        pktlen += MEGA_END_MARK_LEN;
        *(uint16_t *)ptr = MEGA_PKT_END;
        while (pktlen < ETHERNET_MIN_FRAME_LEN) {
          ptr += MEGA_END_MARK_LEN;
          pktlen += MEGA_END_MARK_LEN;
          *(uint16_t *)ptr = MEGA_PKT_END;
        }
        pkt->data_len = pktlen;//client tx_loop中在初始化阶段即可执行
        pkt->pkt_len = pktlen;//client tx_loop中在初始化阶段即可执行

        ip_hdr = (struct rte_ipv4_hdr *)((uint8_t *)rte_pktmbuf_mtod(pkt, uint8_t *) + sizeof(struct rte_ether_hdr));
        ip_hdr->total_length = rte_cpu_to_be_16((uint16_t)(pktlen - sizeof(struct rte_ether_hdr)));
        ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);

        udph = (struct rte_udp_hdr *)((char *)ip_hdr + sizeof(struct rte_ipv4_hdr));
        udph->src_port = rand();
        udph->dst_port = rand();
        //bwb: ↓ ↓ ↓ client tx_loop中在初始化阶段即可执行
        udph->dgram_len =
            rte_cpu_to_be_16((uint16_t)(pktlen - sizeof(struct rte_ether_hdr) - sizeof(struct rte_ipv4_hdr)));
        udph->dgram_cksum = rte_ipv4_udptcp_cksum(ip_hdr, udph);
      };
#ifdef _DUMP_PKT
      auto pkt_content_dump = [&](struct rte_mbuf *pkt) {
        int cnt = 0;
        int pktlen = pkt->data_len - EIU_HEADER_LEN;
        uint16_t *ptr = (uint16_t *)((uint8_t *)rte_pktmbuf_mtod(pkt, uint8_t *) + EIU_HEADER_LEN);
        fprintf(fp[sched_getcpu()], "pkt_len: %d\n", pktlen);
        for (int i = 0; i < pktlen - 2; i += 2) {
          fprintf(fp[sched_getcpu()], "%04x  ", *ptr);
          ptr++;
          if ((++cnt) % 10 == 0) fprintf(fp[sched_getcpu()], "\n");
        }
        fprintf(fp[sched_getcpu()], "END_MARK: %04x \n", *ptr);
        fprintf(fp[sched_getcpu()], "\n");
      };
#endif
      auto pkt_filter = [&](const struct rte_mbuf *pkt) -> bool {
        assert(pkt);
        struct rte_ether_hdr *ethh = (struct rte_ether_hdr *)rte_pktmbuf_mtod(pkt, unsigned char *);
        for (int i = 0; i < 6; i++) {
          if (ethh->d_addr.addr_bytes[i] != S_Addr.addr_bytes[i] ||
              ethh->s_addr.addr_bytes[i] != D_Addr.addr_bytes[i]) {
            return true;
          }
        }
        return false;
      };
      for (i = 0; i < nb_rx; i++) {
        if (pkt_filter(rx_buf[i])) {
          rte_pktmbuf_free(rx_buf[i]);
          // continue;
        }
#ifdef _DUMP_PKT_RECV
        show_pkt(rx_buf[i]);
        pkt_content_dump(rx_buf[i]);
#endif
        ptr = (uint8_t *)((uint8_t *)rte_pktmbuf_mtod(rx_buf[i], uint8_t *) + EIU_HEADER_LEN);
        udph = (struct rte_udp_hdr *)((char *)rte_pktmbuf_mtod(rx_buf[i], char *) + sizeof(struct rte_ether_hdr) +
                                      sizeof(struct rte_ipv4_hdr));
        while (*(uint16_t *)ptr != MEGA_PKT_END) {
          if (*(uint16_t *)ptr == MEGA_JOB_GET) {
            if ((uint32_t)pktlen > (ETHERNET_MAX_FRAME_LEN - RESPOND_ALL_COUNTERS - GET_MAX_RETURN_LEN - MEGA_END_MARK_LEN)) {//bwb:超过GET返回包安全length，暂停解析，先进入发包流程;其中GET_MAX_RETURN_LEN=16，原为22 ???
              complement_pkt(tx_bufs_pt[pkt_id], tx_ptr, pktlen);//此行补全了上一行中的RESPOND_COUNTERSE和END_MARK
              pkt_id++;
              pktlen = EIU_HEADER_LEN;
              if (pkt_id == PKG_GEN_COUNT) {
                for (int k = 0; k < PKG_GEN_COUNT; k++) {
#ifdef _DUMP_PKT_SEND
                  pkt_content_dump(tx_bufs_pt[k]);
#endif
                  check_pkt_end(tx_bufs_pt[k]);
                }
                nb_tx = rte_eth_tx_burst(port, t_id, tx_bufs_pt, PKG_GEN_COUNT);
                core_statistics[core_id].tx += nb_tx;
                pkt_id = 0;
              }
              tx_ptr = (uint8_t *)rte_pktmbuf_mtod(tx_bufs_pt[pkt_id], uint8_t *) + EIU_HEADER_LEN;
              pktlen += 8;  // store response counter in IP pkts.//
              tx_ptr += 8;
            }
            key_len = *(uint16_t *)(ptr + PROTOCOL_TYPE_LEN);
            key_hash = XXH64(ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN, key_len, 1);//bwb:xxh 是一种快速hash算法
            ret = get(table, store, key_hash, ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN, key_len,
                      tx_ptr + PROTOCOL_HEADER_LEN + key_len,//bwb:调用get()写入val
                      (uint32_t *)(tx_ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN), NULL, false);//bwb:此行第一个参数指针的作用时写入val_len
            if (ret) {
              get_succ++;
              val_len = *(uint32_t *)(tx_ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN);
              *(uint16_t *)tx_ptr = GET_SUCC;
              tx_ptr += GET_RESPOND_LEN;
              *(uint16_t *)tx_ptr = key_len;
              tx_ptr += PROTOCOL_KEYLEN_LEN + PROTOCOL_VALLEN_LEN;//
              //bwb: ↑ ↑ ↑ SET_RESPOND_LEN + PROTOCOL_KEYLEN_LEN + PROTOCOL_VALLEN_LEN = PROTOCOL_HEADER_LEN(line 373)
              memcpy(tx_ptr, ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN, key_len);
              tx_ptr += key_len + val_len;
              pktlen += PROTOCOL_HEADER_LEN + key_len + val_len;//此行指347行中的16B/22B?
            } else {
              get_fail++;
              *(uint16_t *)tx_ptr = GET_FAIL;
              tx_ptr += GET_RESPOND_LEN;
              pktlen += GET_RESPOND_LEN;
            }
            ptr += PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN + key_len;
          } else if (*(uint16_t *)ptr == MEGA_JOB_SET) {
            if ((uint32_t)pktlen > (ETHERNET_MAX_FRAME_LEN - RESPOND_ALL_COUNTERS - SET_MAX_RETURN_LEN - MEGA_END_MARK_LEN)) {//bwb:超过SET返回包安全length，暂停解析,先进入发包流程
              complement_pkt(tx_bufs_pt[pkt_id], tx_ptr, pktlen);

              pkt_id++;
              pktlen = EIU_HEADER_LEN;
              if (pkt_id == PKG_GEN_COUNT) {
                for (int k = 0; k < PKG_GEN_COUNT; k++) {
#ifdef _DUMP_PKT_SEND
                  pkt_content_dump(tx_bufs_pt[k]);
#endif
                  check_pkt_end(tx_bufs_pt[k]);
                }
                nb_tx = rte_eth_tx_burst(port, t_id, tx_bufs_pt, PKG_GEN_COUNT);
                core_statistics[core_id].tx += nb_tx;
                pkt_id = 0;
              }
              tx_ptr = (uint8_t *)rte_pktmbuf_mtod(tx_bufs_pt[pkt_id], uint8_t *) + EIU_HEADER_LEN;
              pktlen += 8;
              tx_ptr += 8;
            }
            key_len = *(uint16_t *)(ptr + PROTOCOL_TYPE_LEN);
            val_len = *(uint32_t *)(ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN);
            key_hash = XXH64(ptr + PROTOCOL_HEADER_LEN, key_len, 1);
            // Receive preload pkt with error content
            if (*((uint64_t *)(ptr + PROTOCOL_HEADER_LEN)) !=
                *((uint64_t *)(ptr + PROTOCOL_HEADER_LEN + key_len)) - 1) {
              // ptr += PROTOCOL_HEADER_LEN + key_len + val_len;
              // rte_pktmbuf_dump(stdout, rx_buf[i], rx_buf[i]->pkt_len);
              // show_pkt(rx_buf[i]);
              // assert(false);
            }
            ret = fkv_set(t_id, table, store, key_hash, ptr + PROTOCOL_HEADER_LEN, key_len,
                          ptr + PROTOCOL_HEADER_LEN + key_len, val_len, VALID, false);
            if (ret) {
              set_succ++;
              *(uint16_t *)tx_ptr = SET_SUCC;
              tx_ptr += SET_RESPOND_LEN;
              pktlen += SET_RESPOND_LEN;
              // rte_delay_us_block(2);
              rte_delay_us_sleep(1);
              // printf("SUCC!!!t_id:%d,set_key:%lu\n",core_id,*(uint64_t *)(ptr + PROTOCOL_HEADER_LEN));


            } else {
              set_fail++;
              *(uint16_t *)tx_ptr = SET_FAIL;
              tx_ptr += SET_RESPOND_LEN;
              pktlen += SET_RESPOND_LEN;
              // printf("FAIL!!!t_id:%d,set_key:%lu\n",core_id,*(uint64_t *)(ptr + PROTOCOL_HEADER_LEN));
            }
            ptr += PROTOCOL_HEADER_LEN + key_len + val_len;
          } else {
            rte_pktmbuf_dump(stdout, rx_buf[i], rx_buf[i]->pkt_len);
            // assert(0);
            // core_statistics[core_id].err_ending++;
            break;
          }
        }
        /* if(set_fail)
            printf("t_id:%ld,SET:%d\t%d\n",t_id,set_succ,set_fail); */

        if (pktlen != EIU_HEADER_LEN || pkt_id != 0) {
          complement_pkt(tx_bufs_pt[pkt_id], tx_ptr, pktlen);
          // pkt_id++;//bwb:暂时先注释掉8.30
          for (int k = 0; k < pkt_id; k++) {
#ifdef _DUMP_PKT_SEND
            pkt_content_dump(tx_bufs_pt[k]);
#endif
            check_pkt_end(tx_bufs_pt[k]);
          }
          nb_tx = rte_eth_tx_burst(port, t_id, tx_bufs_pt, pkt_id);
          core_statistics[core_id].tx += nb_tx;
          pkt_id = 0;
          pktlen = EIU_HEADER_LEN;
          tx_ptr = (uint8_t *)rte_pktmbuf_mtod(tx_bufs_pt[pkt_id], uint8_t *) + EIU_HEADER_LEN;
          pktlen += 8;
          tx_ptr += 8;
        }
        rte_pktmbuf_free(rx_buf[i]);
      }
  }
}

extern "C" void set_mt_local(hash_table *table, SlabStore *store, size_t t_id) {
  int core_id = t_id;
  if (set_core_affinity) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(core_id, &mask);
    if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0)
      fprintf(stderr, "[Error] set thread affinity failed\n");
  }
  core_statistics[core_id].enable = 1;

  uint16_t port = 0;
  struct rte_ether_hdr *ethh;
  struct rte_ipv4_hdr *ip_hdr;
  struct rte_udp_hdr *udph;

  static uint32_t round = 50;
  struct rte_mbuf *tx_bufs_pt[PKG_GEN_COUNT * round];

  for (uint32_t i = 0; i < PKG_GEN_COUNT * round; i++) {
    struct rte_mbuf *pkt = (struct rte_mbuf *)rte_pktmbuf_alloc((struct rte_mempool *)send_mbuf_pool);
    if (pkt == NULL) rte_exit(EXIT_FAILURE, "Cannot alloc storage memory in  port %" PRIu16 "\n", port);
    // pkt->data_len = pktlen;
    pkt->nb_segs = 1;  // nb_segs
    // pkt->pkt_len = pkt->data_len;
    pkt->ol_flags = PKT_TX_IPV4;  // ol_flags
    pkt->vlan_tci = 0;            // vlan_tci
    pkt->vlan_tci_outer = 0;      // vlan_tci_outer
    pkt->l2_len = sizeof(struct rte_ether_hdr);
    pkt->l3_len = sizeof(struct rte_ipv4_hdr);

    ethh = (struct rte_ether_hdr *)rte_pktmbuf_mtod(pkt, unsigned char *);
    ethh->s_addr = S_Addr;
    ethh->d_addr = D_Addr;
    ethh->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

    ip_hdr = (struct rte_ipv4_hdr *)((unsigned char *)ethh + sizeof(struct rte_ether_hdr));
    ip_hdr->version_ihl = IP_VHL_DEF;
    ip_hdr->type_of_service = 0;
    ip_hdr->fragment_offset = 0;
    ip_hdr->time_to_live = IP_DEFTTL;
    ip_hdr->next_proto_id = IPPROTO_UDP;
    ip_hdr->packet_id = 0;
    ip_hdr->src_addr = rte_cpu_to_be_32(IP_SRC_ADDR);
    ip_hdr->dst_addr = rte_cpu_to_be_32(IP_DST_ADDR);

    udph = (struct rte_udp_hdr *)((unsigned char *)ip_hdr + sizeof(struct rte_ipv4_hdr));
    tx_bufs_pt[i] = pkt;
  }

  bool ret;
  uint64_t key_hash;
  uint64_t key = 1 + core_id, val = key + 1;
  uint32_t key_len = 8, val_len = 8;
  uint16_t nb_tx;
  int pktlen = EIU_HEADER_LEN, pkt_id = 0;
  uint8_t *tx_ptr = (uint8_t *)rte_pktmbuf_mtod(tx_bufs_pt[pkt_id], uint8_t *) + EIU_HEADER_LEN;

  auto check_pkt_end = [&](struct rte_mbuf *pkt) {
    int pkt_len = pkt->data_len;
    uint16_t *ptr = (uint16_t *)((uint8_t *)rte_pktmbuf_mtod(pkt, uint8_t *) + (pkt_len - 2));
    assert(*ptr == MEGA_PKT_END);

    uint16_t key_len;
    uint32_t val_len;
    char *w_ptr = (char *)rte_pktmbuf_mtod(pkt, char *) + EIU_HEADER_LEN;
    while (*(uint16_t *)w_ptr != MEGA_PKT_END) {
      if (*(uint16_t *)w_ptr == GET_SUCC) {
        key_len = *(uint16_t *)(w_ptr + PROTOCOL_TYPE_LEN);
        val_len = *(uint32_t *)(w_ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN);
        w_ptr += PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN + PROTOCOL_VALLEN_LEN + key_len + val_len;
      } else if (*(uint16_t *)w_ptr == GET_FAIL) {
        w_ptr += SET_RESPOND_LEN;
      } else if (*(uint16_t *)w_ptr == SET_SUCC) {
        w_ptr += SET_RESPOND_LEN;
      } else if (*(uint16_t *)w_ptr == SET_FAIL) {
        w_ptr += SET_RESPOND_LEN;
      } else {
        // break;
        assert(0);
      }
    }
  };

  auto complement_pkt = [&](struct rte_mbuf *pkt, uint8_t *ptr, int pktlen) {
    pktlen += MEGA_END_MARK_LEN;
    *(uint16_t *)ptr = MEGA_PKT_END;
    while (pktlen < ETHERNET_MIN_FRAME_LEN) {
      ptr += MEGA_END_MARK_LEN;
      pktlen += MEGA_END_MARK_LEN;
      *(uint16_t *)ptr = MEGA_PKT_END;
    }
    pkt->data_len = pktlen;
    pkt->pkt_len = pktlen;

    ip_hdr = (struct rte_ipv4_hdr *)((uint8_t *)rte_pktmbuf_mtod(pkt, uint8_t *) + sizeof(struct rte_ether_hdr));
    ip_hdr->total_length = rte_cpu_to_be_16((uint16_t)(pktlen - sizeof(struct rte_ether_hdr)));
    ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);

    udph = (struct rte_udp_hdr *)((char *)ip_hdr + sizeof(struct rte_ipv4_hdr));
    udph->src_port = rand();
    udph->dst_port = rand();
    udph->dgram_len = rte_cpu_to_be_16((uint16_t)(pktlen - sizeof(struct rte_ether_hdr) - sizeof(struct rte_ipv4_hdr)));
  };

  while (table->is_running) {
#ifdef LOCAL_HIT_TEST
// #define ZIPF_THETA 0.99
#define ZIPF_THETA 0.00

    key = t_id;
    val = key + 1;
    uint32_t valLen = 8;
    bool rets = true;
    uint64_t cnts = 0;
    uint64_t set_succ = 0;
    while (key <= 10780343) {
      key_hash = XXH64(&key, key_len, 1);
      rets = fkv_set(t_id, table, store, key_hash, (uint8_t *)&key, key_len, (uint8_t *)&val, valLen, VALID, false);
      if (rets) {
        set_succ++;
      }
      key += NUM_QUEUE;
      val = key + 1;
    }
    uint64_t success = cnts;
    uint64_t upperBound = key - 1;
    struct zipf_gen_state zipf_state;
    mehcached_zipf_init(&zipf_state, upperBound, (double)ZIPF_THETA, (uint64_t)21);
    key = t_id;
    cnts = 0;
    while (key <= upperBound) {
      key_hash = XXH64(&key, key_len, 1);
      rets = get(table, store, key_hash, (uint8_t *)&key, key_len, (uint8_t *)&val, (uint32_t *)&valLen, NULL, false);
      if (rets) {
        cnts++;
      }
      key += NUM_QUEUE;
    }
    printf("== HIT_RATIO ==   id: %02d  hit: %d items / total: %d items  [%d]    == == ==\n", t_id, cnts, set_succ,
           upperBound);
    fflush(stdout);
    while (1) {
      sleep(10);
    }

#endif
    uint32_t cnt = 40;
    while (cnt != 0) {
      // MEGA_JOB_GET
      key_hash = XXH64(&key, key_len, 1);
      ret = get(table, store, key_hash, (uint8_t *)&key, key_len, (uint8_t *)&val,
                (uint32_t *)(tx_ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN), NULL, false);
      if (ret) {
        val_len = *(uint32_t *)(tx_ptr + PROTOCOL_TYPE_LEN + PROTOCOL_KEYLEN_LEN);

        *(uint16_t *)tx_ptr = GET_SUCC;
        tx_ptr += SET_RESPOND_LEN;
        *(uint16_t *)tx_ptr = key_len;
        tx_ptr += PROTOCOL_KEYLEN_LEN + PROTOCOL_VALLEN_LEN;
        memcpy(tx_ptr, &key, key_len);
        tx_ptr += key_len + val_len;
        pktlen += PROTOCOL_HEADER_LEN + key_len + val_len;
      } else {
        *(uint16_t *)tx_ptr = GET_FAIL;
        tx_ptr += SET_RESPOND_LEN;
        pktlen += SET_RESPOND_LEN;
      }
      key = key + 1 + core_id;
      val = key + 1;
      cnt--;
      // MEGA_JOB_SET
      key_hash = XXH64(&key, key_len, 1);
      if (key != val - 1) {
        assert(false);
      }
      ret = fkv_set(t_id, table, &datastore.slab_store[t_id], key_hash, (uint8_t *)&key, key_len, (uint8_t *)&val,
                    val_len, VALID, false);

      if (ret) {
        *(uint16_t *)tx_ptr = SET_SUCC;
        tx_ptr += SET_RESPOND_LEN;
        pktlen += SET_RESPOND_LEN;
      } else {
        *(uint16_t *)tx_ptr = SET_FAIL;
        tx_ptr += SET_RESPOND_LEN;
        pktlen += SET_RESPOND_LEN;
      }
    }

    complement_pkt(tx_bufs_pt[pkt_id], tx_ptr, pktlen);
    check_pkt_end(tx_bufs_pt[pkt_id]);
    nb_tx = rte_eth_tx_burst(port, t_id, &tx_bufs_pt[pkt_id], 1);
    core_statistics[core_id].tx += nb_tx;
    pkt_id = (pkt_id + PKG_GEN_COUNT) % (PKG_GEN_COUNT * round);
    pktlen = EIU_HEADER_LEN;
    tx_ptr = (uint8_t *)rte_pktmbuf_mtod(tx_bufs_pt[pkt_id], uint8_t *) + EIU_HEADER_LEN;
  }
}

extern "C" void sigint_handler(int sig) {
  
  __sync_bool_compare_and_swap((volatile Cbool *)&mytable.is_running, 1U, 0U);
  for (auto &t : workers) t.join();
  print_table_stats(&mytable);
  printf("[INFO] Everything works fine.\n");
  for(int i=0;i<4;i++) printf("rx_queue_%d:%ld\n",i,core_statistics[i].rx);
  fflush(stdout);
  show_system_status(&mytable);
  shm_free_all();
  exit(EXIT_SUCCESS);
}

extern "C" void test_basic(size_t log_pages, size_t multiple, size_t flow_mode, size_t flow_pages) {//log_pages=20,multiple未用
  printf("[T]: Testing expandable circular log...\n");

  slabStore_init(&datastore, log_pages);

  /* Initiate table instance with the memory left after initiating log. */
  hash_table_init(&mytable, &datastore);

  show_system_status(&mytable);

  size_t id;
  for (id = 0; id < NUM_QUEUE; id++) {
    if (id == 0) printf(" == [STAT] Workers Start (%d threads in total) == \n", NUM_QUEUE);
#ifndef LOCAL_HIT_TEST
    workers.push_back(std::thread(set_mt, &mytable, &datastore.slab_store[id], id));
#else
    workers.push_back(std::thread(set_mt_local, &mytable, &datastore.slab_store[id], id));
#endif
  }

  if (flow_mode == 3) workers.push_back(std::thread(mem_flowing_controler, &mytable));
#ifdef BUCKET_CLEANER
    // workers.push_back(std::thread(clean_expired_entry, &mytable, set_core_affinity));
    // workers.push_back(std::thread(clean_expired_item, &mytable, set_core_affinity));
#endif

  for (auto &t : workers) {
    t.join();
  }
  show_system_status(&mytable);
}

int main(int argc, char *argv[]) {
#ifdef _DUMP_PKT
  char filename[50];
  for (int i = 0; i < NUM_MAX_CORE; i++) {
    sprintf(filename, "/home/hewen/tmp/mac_%d.txt", i);
    if ((fp[i] = fopen(filename, "wt+")) == NULL) {
      printf("Fail to open file(%s)\n", filename);
      exit(-1);
    }
  }
#endif

  int ch;
  size_t multiple = 10;
  size_t pages = 20;
  size_t flow_mode = 0, flow_pages = 0;
  while ((ch = getopt(argc, argv, "hcs:p:f:n:")) != -1) {
    switch (ch) {
      case 'h':
        printf(
            " Usage: %s [-s <workload>] [-p <pages for storage>] "
            "[-f <flow "
            "mode>] [-n <flow pages>]\n",
            argv[0]);
        exit(0);
      case 's':
        multiple = atoi(optarg);
        break;
      case 'p':
        pages = atoi(optarg);
        break;
      case 'f':
        flow_mode = atoi(optarg);
        break;
      case 'n':
        flow_pages = atoi(optarg);
        break;
      case 'c':
        set_core_affinity = 0;
        break;
      default:
        printf("Error: unknown option: %c\n", (char)optopt);
        break;
    }
  }
  std::signal(SIGINT, sigint_handler);

  shm_init(page_size, num_pages_to_try);

  port_init();

  test_basic(pages, multiple, flow_mode, flow_pages);



  return EXIT_SUCCESS;
}

extern "C" void port_init() {
  unsigned nb_ports;

  /* Initialize the Environment Abstraction Layer (EAL). */
  int t_argc = 8;
  char *t_argv[] = {(char *)"./build/benchmark",
                    (char *)"-c",
                    (char *)"f",
                    (char *)"-n",
                    (char *)"1",
                    (char *)"--huge-unlink",
                    (char *)"-w",
                    (char *)"pci@0000:03:00.1"};
  int ret = rte_eal_init(t_argc, t_argv);
  if (ret < 0) rte_exit(EXIT_FAILURE, "Error with EAL initialization\n");

  nb_ports = rte_eth_dev_count_avail();
  printf("WEN: There are %d device(s) available\n", nb_ports);

  /* Creates a new mempool in memory to hold the mbufs. */
  char str[15];
  for (uint32_t i = 0; i < NUM_QUEUE; i++) {
    sprintf(str, "RX_POOL_%d", i);
    recv_mbuf_pool[i] = rte_pktmbuf_pool_create(str, NUM_MBUFS * nb_ports, MBUF_CACHE_SIZE, 0,
                                                RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
    if (recv_mbuf_pool[i] == NULL) rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");
  }
  send_mbuf_pool = rte_pktmbuf_pool_create("SEND_POOL", NUM_MBUFS * nb_ports, MBUF_CACHE_SIZE, 0,
                                           RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
  if (send_mbuf_pool == NULL) rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");

  /* Initialize all ports. */
  const uint16_t rx_rings = NUM_QUEUE, tx_rings = NUM_QUEUE;
  uint16_t nb_rxd = RX_RING_SIZE;
  uint16_t nb_txd = TX_RING_SIZE;
  uint16_t q;
  int retval;
  const int port = 0;
  struct rte_eth_conf port_conf_default;
  memset(&port_conf_default, 0, sizeof(struct rte_eth_conf));
  port_conf_default.rxmode.mq_mode = ETH_MQ_RX_RSS;
  port_conf_default.rxmode.max_rx_pkt_len = RTE_ETHER_MAX_LEN;
  port_conf_default.rxmode.offloads = DEV_RX_OFFLOAD_IPV4_CKSUM;
  port_conf_default.rx_adv_conf.rss_conf.rss_key = NULL;
  port_conf_default.rx_adv_conf.rss_conf.rss_hf = ETH_RSS_IP | ETH_RSS_UDP;
  port_conf_default.txmode.mq_mode = ETH_MQ_TX_NONE;
  struct rte_eth_conf port_conf = port_conf_default;
  struct rte_eth_dev_info dev_info;
  struct rte_eth_txconf tx_conf;
  retval = rte_eth_dev_info_get(port, &dev_info);
  if (retval < 0) {
    rte_exit(EXIT_FAILURE, "Error during getting device (port %u) info: %s\n", port, strerror(-retval));
  }
  tx_conf = dev_info.default_txconf;
  tx_conf.offloads = port_conf.txmode.offloads;

  /* Configure the Ethernet device. */
  retval = rte_eth_dev_configure(port, rx_rings, tx_rings, &port_conf);
  if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot configure device: err=%d, port=%u\n", retval, (unsigned)port);
  retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd);
  if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot configure adjust desc: port=%u\n", (unsigned)port);

  /* Allocate and set up RX queue(s) per Ethernet port. */
  for (q = 0; q < rx_rings; q++) {
    retval = rte_eth_rx_queue_setup(port, q, nb_rxd, rte_eth_dev_socket_id(port), NULL, recv_mbuf_pool[q]);
    if (retval < 0) rte_exit(EXIT_FAILURE, "rte_eth_rx_queue_setup:err=%d, port=%u\n", retval, (unsigned)port);
  }

  /* Allocate and set up TX queue(s) per Ethernet port. */
  for (q = 0; q < tx_rings; q++) {
    retval = rte_eth_tx_queue_setup(port, q, nb_txd, rte_eth_dev_socket_id(port), &tx_conf);
    if (retval < 0) rte_exit(EXIT_FAILURE, "rte_eth_tx_queue_setup:err=%d, port=%u\n", retval, (unsigned)port);
  }

  retval = rte_eth_dev_start(port);
  if (retval < 0) rte_exit(EXIT_FAILURE, "rte_eth_dev_start:err=%d, port=%u\n", retval, (unsigned)port);
  rte_eth_promiscuous_enable(port);
}

extern "C" inline void show_mac(const char *info, struct rte_ether_addr *addr) {
#ifdef _DUMP_PKT_RECV
  fprintf(fp[sched_getcpu()],
#else
  printf(
#endif
          "%s %02" PRIx8 " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n", info,
          (addr->addr_bytes)[0], (addr->addr_bytes)[1], (addr->addr_bytes)[2], (addr->addr_bytes)[3],
          (addr->addr_bytes)[4], (addr->addr_bytes)[5]);
}

extern "C" void show_system_status(const hash_table *table) {
  const double factor = 1.0;

  uint64_t table_capa = table->num_partitions * ITEMS_PER_BUCKET * BUCKETS_PER_PARTITION;
  uint64_t item_sum = 0;
  for (uint32_t i = 0; i < table->stores->numStores; i++) {
    item_sum += table->stores->slab_store[i].tstats.count;
  }
  double load_factor = item_sum * factor / table_capa;
  // double log_load

  printf("\n");
  printf("count:                  %10zu\n", item_sum);
  printf("table_capa:             %10zu | ", table_capa);
  printf("load_factor:            %10lf\n", load_factor);
  // printf("part_pages:             %10u | ", table->num_partitions);
  // printf("store_pages:              %10zu\n", ;
  // printf("store_load:               %10lf | ", log_load);
  printf("\n");
}

/* Print out statistics on packets dropped */
extern "C" void print_stats() {
  static uint64_t total_err_ending = 0;
  uint64_t total_packets_dropped, total_packets_tx, total_packets_rx;
  unsigned core_id;

  total_packets_dropped = 0;
  total_packets_tx = 0;
  total_packets_rx = 0;

  const char clr[] = {27, '[', '2', 'J', '\0'};
  const char topLeft[] = {27, '[', '1', ';', '1', 'H', '\0'};

  /* Clear screen and move to top left */
  printf("%s%s", clr, topLeft);

  printf("\nPort statistics ====================================");

  for (core_id = 0; core_id < NUM_MAX_CORE; core_id++) {
    if (core_statistics[core_id].enable == 0) continue;
    printf(
        "\nStatistics for core %d ------------------------------"
        "    Packets sent: %11" PRIu64 "    Packets received: %11" PRIu64 "    Packets dropped: %11" PRIu64,
        core_id, core_statistics[core_id].tx, core_statistics[core_id].rx, core_statistics[core_id].dropped);

    total_packets_dropped += core_statistics[core_id].dropped;
    total_packets_tx += core_statistics[core_id].tx;
    total_packets_rx += core_statistics[core_id].rx;
    total_err_ending += core_statistics[core_id].err_ending;

    core_statistics[core_id].err_ending = 0;
    core_statistics[core_id].dropped = 0;
    core_statistics[core_id].tx = 0;
    core_statistics[core_id].rx = 0;
  }

  printf(
      "\nAggregate statistics ==============================="
      "\nTotal packets sent: %18" PRIu64 "\nTotal packets received: %14" PRIu64 "\nTotal packets dropped: %15" PRIu64
      "\nTotal packets err_end: %15" PRIu64,
      total_packets_tx, total_packets_rx, total_packets_dropped, total_err_ending);
  printf("\n====================================================\n");
}
