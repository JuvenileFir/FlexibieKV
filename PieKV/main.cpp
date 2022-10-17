#include "piekv.hpp"

int main(){
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
        pages = atoi(optarg);//log_pages=120
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


	printf("[T]: Testing expandable circular log...\n");

  slabStore_init(&datastore, pages);

  /* Initiate table instance with the memory left after initiating log. */
  hash_table_init(&mytable, &datastore);

  // show_system_status(&mytable);

  size_t id;
  for (id = 0; id < NUM_QUEUE; id++) {
    if (id == 0) printf(" == [STAT] Workers Start (%d threads in total) == \n", NUM_QUEUE);
	    workers.push_back(std::thread(set_mt, &mytable, &datastore.slab_store[id], id));
  }

  if (flow_mode == 3) workers.push_back(std::thread(mem_flowing_controler, &mytable));

  for (auto &t : workers) {
    t.join();
  }
  show_system_status(&mytable);

  return EXIT_SUCCESS;
}