
#ifndef COMMUNICATION_HPP_
#define COMMUNICATION_HPP_

#include <cstdint>

#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_byteorder.h>
#include <rte_ip.h>
#include <rte_ether.h>


#include "piekv.hpp"

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
#define PROTOCOL_KEYHASHLEN_LEN 2U
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

#define NUM_QUEUE THREAD_NUM
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


#define NUM_MAX_CORE 36
// const size_t page_size = 1048576 * 2;  // page size = 2 MiB
const size_t kblock_size = 2097152UL;
const size_t num_mem_blocks = 240;   // SHM_MAX_PAGES;

struct benchmark_core_statistics {
  uint64_t tx;
  uint64_t rx;
  uint64_t dropped;
  uint64_t err_ending;
  int enable;
} __rte_cache_aligned;

static struct benchmark_core_statistics core_statistics[NUM_MAX_CORE];

static size_t set_core_affinity = 1;


typedef struct RxGet_Packet
{
    uint16_t operation_type;
    uint16_t key_len;
    uint16_t key_hash_len;
}RxGet_Packet;


typedef struct RxSet_Packet
{
    uint16_t operation_type;
    uint16_t key_len;
    uint16_t key_hash_len;
    uint16_t val_len;
}RxSet_Packet;


typedef struct TxGet_Packet
{
    uint16_t result;
    uint16_t key_len;
    uint32_t val_len;
}TxGet_Packet;


typedef struct RT_Counter
{
     uint16_t get_succ = 0;
     uint16_t set_succ = 0;
     uint16_t get_fail = 0;
     uint16_t set_fail = 0;
}RT_Counter;




class RTWorker
{
private:
    /* data */
    // communication
    struct rte_mbuf *tx_bufs_pt[PKG_GEN_COUNT];
    struct rte_mbuf *rx_buf[BURST_SIZE];
   
    int pktlen = EIU_HEADER_LEN, pkt_id = 0;

    RT_Counter rt_counter_;

    size_t t_id_;
    int core_id;
    uint8_t *ptr = NULL;
    uint16_t port = 0;
    struct rte_ether_hdr *ethh;
    struct rte_ipv4_hdr *ip_hdr;
    struct rte_udp_hdr *udph;
    struct rte_mbuf *pkt;
    uint16_t nb_rx, nb_tx;
    uint8_t *tx_ptr;

    Piekv *piekv_;

public:
    RTWorker(Piekv *piekv, size_t t_id, struct rte_mempool *send_mbuf_pool);
    ~RTWorker();
    void parse_get();
    void parse_set();
    void worker_proc();

    void complement_pkt(struct rte_mbuf *pkt, uint8_t *ptr, int pktlen);
    void check_pkt_end(struct rte_mbuf *pkt);
    bool pkt_filter(const struct rte_mbuf *pkt);

    void send_packet();
};









#endif