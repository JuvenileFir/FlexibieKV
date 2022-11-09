#include "communication.hpp"


struct rte_mempool *kRecv_mbuf_pool[THREAD_NUM];

struct rte_mempool *send_mbuf_pool;

typedef struct context_s {
  unsigned int core_id;
  unsigned int queue_id;
} context_t;


std::vector<std::thread> workers;
Piekv *m_piekv;
Timer *timer;

void sigint_handler(int sig) {
  printf("run finish job......\n");
  printf("piekv run: %d",m_piekv->is_running_);
  __sync_bool_compare_and_swap((volatile uint32_t *)&(m_piekv->is_running_), 1U, 0U);
  for (auto &t : workers) t.join();
  printf("\n");
  for (int i = 0; i < THREAD_NUM; i++) {
    m_piekv->log_->log_segments_[i]->print_table_stats();
    printf("\n\n");
  }
  // print_table_stats(&mytable);
  printf("[INFO] Everything works fine.\n");
  for(int i=0;i<4;i++) printf("rx_queue_%d:%ld\n",i,core_statistics[i].rx);
  fflush(stdout);
  // TODO:show table status here    show_system_status(&mytable);
  // TODO:free all shm_free_all();
  exit(EXIT_SUCCESS);
}

/* template <typename Signal, typename... Args>
void addSignal(Signal& sigset, Args... args) {
  (sigaddset(&sigset, args), ...);
} */

void port_init() {
  unsigned nb_ports;
  printf("enter the port\n");
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
  for (uint32_t i = 0; i < THREAD_NUM; i++) {
    sprintf(str, "RX_POOL_%d", i);
    kRecv_mbuf_pool[i] = rte_pktmbuf_pool_create(str, NUM_MBUFS * nb_ports, MBUF_CACHE_SIZE, 0,
                                                RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
    if (kRecv_mbuf_pool[i] == NULL) rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");
  }
  send_mbuf_pool = rte_pktmbuf_pool_create("SEND_POOL", NUM_MBUFS * nb_ports, MBUF_CACHE_SIZE, 0,
                                           RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
  if (send_mbuf_pool == NULL) rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");

  /* Initialize all ports. */
  const uint16_t rx_rings = THREAD_NUM, tx_rings = THREAD_NUM;
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
    retval = rte_eth_rx_queue_setup(port, q, nb_rxd, rte_eth_dev_socket_id(port), NULL, kRecv_mbuf_pool[q]);
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

void print_piekv()
{
  printf("piekv run: %d",m_piekv->is_running_);
}



int main(int argc, char *argv[]){
    int ch;
    // size_t multiple = 10;
    size_t pages = 20;
    size_t flow_mode = 0/* , flow_pages = 0 */;
    while ((ch = getopt(argc, argv, "hcs:p:f:n:")) != -1) {
        switch (ch) {
        case 'h':
            printf(
                " Usage: %s [-s <workload>] [-p <pages for storage>] "
                "[-f <flow "
                "mode>] [-n <flow pages>]\n",
                argv[0]);
            exit(0);
/*         case 's':
            multiple = atoi(optarg);
            break; */
        case 'p':
            pages = atoi(optarg);//log_pages=120
            break;
        case 'f':
            flow_mode = atoi(optarg);
            break;
/*         case 'n':
            flow_pages = atoi(optarg);
            break; */
        case 'c':
            set_core_affinity = 0;
            break;
        default:
            printf("Error: unknown option: %c\n", (char)optopt);
            break;
        }
    }

    // std::signal(SIGINT, sigint_handler);

    port_init();
    m_piekv = new Piekv(pages, kblock_size, num_mem_blocks);

    timer = new Timer();

    // show_system_status(&mytable);
    RTWorker *m_rtworkers[4];

    printf(" == [STAT] Workers Start (%d threads in total) == \n", THREAD_NUM);
    size_t id;
    // GetThread();
    for (id = 0; id < THREAD_NUM; id++) {

            m_rtworkers[id] = new RTWorker(m_piekv, id, send_mbuf_pool);

            workers.push_back(std::thread(&RTWorker::worker_proc,m_rtworkers[id]));
    }
    
    if (flow_mode == 3) workers.push_back(std::thread(&Piekv::memFlowingController, m_piekv));

    // show_system_status(&mytable);
    // TODO: delete all new here and in signal
    int input = 123;
    while(scanf("%d",&input)){
      if (input == 0){     
        printf("running finish job......\n");
        __sync_bool_compare_and_swap((volatile uint32_t *)&(m_piekv->is_running_), 1U, 0U);
        for (auto &t : workers)
          t.join();
        printf("\n");
        for (int i = 0; i < THREAD_NUM; i++)
        {
          m_piekv->log_->log_segments_[i]->print_table_stats();
          printf("\n\n");
        }
        /* for (int i = 0; i < 36; i++)
        {
          printf("[main]End rx_queue_%ld:%ld\n", i, core_statistics[i].rx);
          printf("\n");
        } */
        printf("[INFO] Everything works fine.\n");fflush(stdout);
        exit(EXIT_SUCCESS); 
      }
      if (input == 1) {
        for (int i = 0; i < THREAD_NUM; i++)
        {
          m_piekv->log_->log_segments_[i]->print_table_stats();
          printf("\n\n");
        }
        timer->showTime();
      }
      if (input == 2) {
        timer->showTime();
        // timer->clear();
        for (int i = 0; i < 4; i++) {
          printf("thread %d rx: %d\n",i,core_statistics[i].rx);
        }
        m_piekv->showUtilization();
      }
      if (input == 3) {
        timer->showCount();
      }
    }


    for (auto &t : workers) {
        t.join();
    }
/*     printf("\n");
    for (int i = 0; i < THREAD_NUM; i++)
    {
      m_piekv->log_->log_segments_[i]->print_table_stats();
      printf("\n\n");
    }
    printf("[INFO] Everything works fine.\n");fflush(stdout); */

    return EXIT_SUCCESS;
}