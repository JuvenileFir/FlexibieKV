/* Partition keys basing on its hash value */

#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "partition.h"
// #include "basic_log.h"

EXTERN_BEGIN

uint64_t num_long_arcs_[2];
uint64_t num_short_arc_groups_[2];
uint64_t num_short_arcs_[2];
uint64_t current_s_[2];//当前的group_size
uint64_t S_;//group_size_lower_bound,upper bound is double S_
uint64_t S_minus_one;
uint64_t S_log_;
uint64_t arc_groups_[2];
uint64_t num_buckets_;
uint64_t a_, b_;

uint64_t lh_n, lh_n0, lh_l, lh_p, lh_x;

static size_t HashToArc(uint64_t hash, Cbool version);
static size_t ArcToBucket(size_t arc_num, Cbool version);

static uint64_t hash_param;

static PartitionMap partition_map;

uint64_t rotl(uint64_t x, int k) { return (x << k) | (x >> (64 - k)); }

void print_partiton_stats(Cbool version) {
  // printf("[STAT BEGIN]\n");
  // printf("         Parts         long_arcs         short_arcs current_s
  // short_arc_g         arc_g\n");
  printf("%-10lu\t", NumBuckets_v(version));
  printf("%-14lu\t", num_long_arcs_[version]);
  printf("%-14lu\t", num_short_arcs_[version]);
  printf("%-14lu\t", current_s_[version]);
  printf("%-14lu\t", num_short_arc_groups_[version]);
  printf("%-10lu\t", arc_groups_[version]);
  printf("\n");
  fflush(stdout);
}

static inline uint64_t HashValue(uint64_t val, uint64_t mod_pow) UNUSED;
static inline uint64_t HashValue(uint64_t val, uint64_t mod_pow) {
  uint64_t s0 = val;
  uint64_t s1 = hash_param;

  s1 ^= s0;
  s0 = rotl(s0, 55) ^ s1 ^ (s1 << 14);  // a, b
  s1 = rotl(s1, 36);                    // c
  uint64_t hash1 = s0 + s1;
  hash1 = hash1 & ((1ULL << mod_pow) - 1);
  return hash1;
}

#ifdef HARDCODE_DIVISION

#define DIVF(i) \
  static uint64_t div##i(uint64_t a) { return a / i; }

DIVF(1);
DIVF(2);
DIVF(3);
DIVF(4);
DIVF(5);
DIVF(6);
DIVF(7);
DIVF(8);
DIVF(9);
DIVF(10);
DIVF(11);
DIVF(12);
DIVF(13);
DIVF(14);
DIVF(15);
DIVF(16);
DIVF(17);
DIVF(18);
DIVF(19);
DIVF(20);
DIVF(21);
DIVF(22);
DIVF(23);
DIVF(24);
DIVF(25);
DIVF(26);
DIVF(27);
DIVF(28);
DIVF(29);
DIVF(30);
DIVF(31);
DIVF(32);
DIVF(33);
DIVF(34);
DIVF(35);
DIVF(36);
DIVF(37);
DIVF(38);
DIVF(39);
DIVF(40);
DIVF(41);
DIVF(42);
DIVF(43);
DIVF(44);
DIVF(45);
DIVF(46);
DIVF(47);
DIVF(48);
DIVF(49);
DIVF(50);
DIVF(51);
DIVF(52);
DIVF(53);
DIVF(54);
DIVF(55);
DIVF(56);
DIVF(57);
DIVF(58);
DIVF(59);
DIVF(60);
DIVF(61);
DIVF(62);
DIVF(63);
DIVF(64);
DIVF(65);
DIVF(66);
DIVF(67);
DIVF(68);
DIVF(69);
DIVF(70);
DIVF(71);
DIVF(72);
DIVF(73);
DIVF(74);
DIVF(75);
DIVF(76);
DIVF(77);
DIVF(78);
DIVF(79);
DIVF(80);
DIVF(81);
DIVF(82);
DIVF(83);
DIVF(84);
DIVF(85);
DIVF(86);
DIVF(87);
DIVF(88);
DIVF(89);
DIVF(90);
DIVF(91);
DIVF(92);
DIVF(93);
DIVF(94);
DIVF(95);
DIVF(96);
DIVF(97);
DIVF(98);
DIVF(99);
DIVF(100);
DIVF(101);
DIVF(102);
DIVF(103);
DIVF(104);
DIVF(105);
DIVF(106);
DIVF(107);
DIVF(108);
DIVF(109);
DIVF(110);
DIVF(111);
DIVF(112);
DIVF(113);
DIVF(114);
DIVF(115);
DIVF(116);
DIVF(117);
DIVF(118);
DIVF(119);
DIVF(120);
DIVF(121);
DIVF(122);
DIVF(123);
DIVF(124);
DIVF(125);
DIVF(126);
DIVF(127);
DIVF(128);
DIVF(129);
DIVF(130);
DIVF(131);
DIVF(132);
DIVF(133);
DIVF(134);
DIVF(135);
DIVF(136);
DIVF(137);
DIVF(138);
DIVF(139);
DIVF(140);
DIVF(141);
DIVF(142);
DIVF(143);
DIVF(144);
DIVF(145);
DIVF(146);
DIVF(147);
DIVF(148);
DIVF(149);
DIVF(150);
DIVF(151);
DIVF(152);
DIVF(153);
DIVF(154);
DIVF(155);
DIVF(156);
DIVF(157);
DIVF(158);
DIVF(159);
DIVF(160);
DIVF(161);
DIVF(162);
DIVF(163);
DIVF(164);
DIVF(165);
DIVF(166);
DIVF(167);
DIVF(168);
DIVF(169);
DIVF(170);
DIVF(171);
DIVF(172);
DIVF(173);
DIVF(174);
DIVF(175);
DIVF(176);
DIVF(177);
DIVF(178);
DIVF(179);
DIVF(180);
DIVF(181);
DIVF(182);
DIVF(183);
DIVF(184);
DIVF(185);
DIVF(186);
DIVF(187);
DIVF(188);
DIVF(189);
DIVF(190);
DIVF(191);
DIVF(192);
DIVF(193);
DIVF(194);
DIVF(195);
DIVF(196);
DIVF(197);
DIVF(198);
DIVF(199);
DIVF(200);
DIVF(201);
DIVF(202);
DIVF(203);
DIVF(204);
DIVF(205);
DIVF(206);
DIVF(207);
DIVF(208);
DIVF(209);
DIVF(210);
DIVF(211);
DIVF(212);
DIVF(213);
DIVF(214);
DIVF(215);
DIVF(216);
DIVF(217);
DIVF(218);
DIVF(219);
DIVF(220);
DIVF(221);
DIVF(222);
DIVF(223);
DIVF(224);
DIVF(225);
DIVF(226);
DIVF(227);
DIVF(228);
DIVF(229);
DIVF(230);
DIVF(231);
DIVF(232);
DIVF(233);
DIVF(234);
DIVF(235);
DIVF(236);
DIVF(237);
DIVF(238);
DIVF(239);
DIVF(240);
DIVF(241);
DIVF(242);
DIVF(243);
DIVF(244);
DIVF(245);
DIVF(246);
DIVF(247);
DIVF(248);
DIVF(249);
DIVF(250);
DIVF(251);
DIVF(252);
DIVF(253);
DIVF(254);
DIVF(255);
DIVF(256);
DIVF(257);
DIVF(258);
DIVF(259);

uint64_t (*divf[])(uint64_t) = {
    NULL,   div1,   div2,   div3,   div4,   div5,   div6,   div7,   div8,   div9,   div10,  div11,  div12,  div13,
    div14,  div15,  div16,  div17,  div18,  div19,  div20,  div21,  div22,  div23,  div24,  div25,  div26,  div27,
    div28,  div29,  div30,  div31,  div32,  div33,  div34,  div35,  div36,  div37,  div38,  div39,  div40,  div41,
    div42,  div43,  div44,  div45,  div46,  div47,  div48,  div49,  div50,  div51,  div52,  div53,  div54,  div55,
    div56,  div57,  div58,  div59,  div60,  div61,  div62,  div63,  div64,  div65,  div66,  div67,  div68,  div69,
    div70,  div71,  div72,  div73,  div74,  div75,  div76,  div77,  div78,  div79,  div80,  div81,  div82,  div83,
    div84,  div85,  div86,  div87,  div88,  div89,  div90,  div91,  div92,  div93,  div94,  div95,  div96,  div97,
    div98,  div99,  div100, div101, div102, div103, div104, div105, div106, div107, div108, div109, div110, div111,
    div112, div113, div114, div115, div116, div117, div118, div119, div120, div121, div122, div123, div124, div125,
    div126, div127, div128, div129, div130, div131, div132, div133, div134, div135, div136, div137, div138, div139,
    div140, div141, div142, div143, div144, div145, div146, div147, div148, div149, div150, div151, div152, div153,
    div154, div155, div156, div157, div158, div159, div160, div161, div162, div163, div164, div165, div166, div167,
    div168, div169, div170, div171, div172, div173, div174, div175, div176, div177, div178, div179, div180, div181,
    div182, div183, div184, div185, div186, div187, div188, div189, div190, div191, div192, div193, div194, div195,
    div196, div197, div198, div199, div200, div201, div202, div203, div204, div205, div206, div207, div208, div209,
    div210, div211, div212, div213, div214, div215, div216, div217, div218, div219, div220, div221, div222, div223,
    div224, div225, div226, div227, div228, div229, div230, div231, div232, div233, div234, div235, div236, div237,
    div238, div239, div240, div241, div242, div243, div244, div245, div246, div247, div248, div249, div250, div251,
    div252, div253, div254, div255, div256, div257, div258, div259};

#endif

size_t ArcNum(uint64_t divs, uint64_t hash) {
  uint64_t divs1 = divs >> 32;
  uint64_t divs0 = divs & 0xffffffff;
  uint64_t hash1 = hash >> 32;
  uint64_t hash0 = hash & 0xffffffff;
  uint64_t low = (hash0 * divs0) >> 32;
  size_t new_num = (hash1 * divs1) + ((hash1 * divs0 + hash0 * divs1 + low) >> 32);

  return new_num;
}

size_t RoundHash(uint64_t value, Cbool version) {
#ifndef ASSUME_HASHED
  value = HashValue(value, 60);
#endif
  const size_t arc = HashToArc(value, version);
  const size_t bucket = ArcToBucket(arc, version);
  return bucket;
}

/* For two version info */
uint64_t NumBuckets_v(Cbool version) { return num_long_arcs_[version] + num_short_arcs_[version]; }

void NewBucket(Cbool version) {
  assert(num_short_arcs_[version] == num_short_arc_groups_[version] * (current_s_[version] + 1));
  // Simple case: we can change all the hashes
  if (NumBuckets_v(version) < S_) {
    num_long_arcs_[version]++;
    return;
  }
  num_long_arcs_[version] -= current_s_[version];
  num_short_arc_groups_[version]++;
  num_short_arcs_[version] += current_s_[version] + 1;
  // If we are done going around the circle ...
  if (num_long_arcs_[version] == 0) {
    num_long_arcs_[version] = num_short_arcs_[version];
    num_short_arc_groups_[version] = 0;
    num_short_arcs_[version] = 0;
    current_s_[version]++;
  }
  // If we completed a doubling...
  if (current_s_[version] == 2 * S_) {
    current_s_[version] = S_;
    arc_groups_[version] *= 2;
  }
}

void DelBucket(Cbool version) {
  assert(NumBuckets_v(version));
  // Simple case: we can change all the hashes
  if (NumBuckets_v(version) <= S_) {
    num_long_arcs_[version]--;
    return;
  }
  // If we completed a doubling.
  if (current_s_[version] == S_ && !num_short_arcs_[version]) {
    current_s_[version] = (S_ << 1);
    arc_groups_[version] >>= 1;
  }
  // If we are done going around the circle.
  if (num_short_arcs_[version] == 0) {
    num_short_arcs_[version] = num_long_arcs_[version];
    num_short_arc_groups_[version] = arc_groups_[version];
    num_long_arcs_[version] = 0;
    current_s_[version]--;
  }
  num_short_arcs_[version] -= current_s_[version] + 1;
  num_short_arc_groups_[version]--;
  num_long_arcs_[version] += current_s_[version];
}

void NewBucket_v(Cbool version) {
  assert(num_short_arcs_[version] == num_short_arc_groups_[version] * (current_s_[version] + 1));
  // Simple case: we can change all the hashes
  if (NumBuckets_v(version) < S_) {
    num_long_arcs_[!version] = num_long_arcs_[version] + 1;
    num_short_arc_groups_[!version] = num_short_arc_groups_[version];
    num_short_arcs_[!version] = num_short_arcs_[version];
    current_s_[!version] = current_s_[version];
    arc_groups_[!version] = arc_groups_[version];
    return;
  }
  num_long_arcs_[!version] = num_long_arcs_[version] - current_s_[version];
  num_short_arc_groups_[!version] = num_short_arc_groups_[version] + 1;
  num_short_arcs_[!version] = num_short_arcs_[version] + current_s_[version] + 1;
  current_s_[!version] = current_s_[version];
  arc_groups_[!version] = arc_groups_[version];
  // If we are done going around the circle ...
  if (num_long_arcs_[!version] == 0) {
    num_long_arcs_[!version] = num_short_arcs_[!version];
    num_short_arc_groups_[!version] = 0;
    num_short_arcs_[!version] = 0;
    current_s_[!version]++;
  }
  // If we completed a doubling...
  if (current_s_[!version] == 2 * S_) {
    current_s_[!version] = S_;
    arc_groups_[!version] *= 2;
  }
}

void DelBucket_v(Cbool version) {
  assert(NumBuckets_v(version));
  // Simple case: we can change all the hashes
  if (NumBuckets_v(version) <= S_) {
    num_long_arcs_[!version] = num_long_arcs_[version] - 1;
    num_short_arc_groups_[!version] = num_short_arc_groups_[version];
    num_short_arcs_[!version] = num_short_arcs_[version];
    current_s_[!version] = current_s_[version];
    arc_groups_[!version] = arc_groups_[version];
    return;
  }
  current_s_[!version] = current_s_[version];
  arc_groups_[!version] = arc_groups_[version];
  num_short_arc_groups_[!version] = num_short_arc_groups_[version];
  num_long_arcs_[!version] = num_long_arcs_[version];
  num_short_arcs_[!version] = num_short_arcs_[version];
  // If we completed a doubling.
  if (current_s_[version] == S_ && !num_short_arcs_[version]) {
    current_s_[!version] = (S_ << 1);
    arc_groups_[!version] >>= 1;
  }
  // If we are done going around the circle.
  if (num_short_arcs_[version] == 0) {
    num_short_arcs_[!version] = num_long_arcs_[version];
    num_short_arc_groups_[!version] = arc_groups_[!version];
    num_long_arcs_[!version] = 0;
    current_s_[!version]--;
  }
  num_short_arcs_[!version] = num_short_arcs_[!version] - (current_s_[!version] + 1);
  num_short_arc_groups_[!version] = num_short_arc_groups_[!version] - 1;
  num_long_arcs_[!version] = num_long_arcs_[!version] + current_s_[!version];
}

void hash_init_partition(const Cbool version, uint64_t S, uint64_t num_buckets) {
  S_ = S;
  num_long_arcs_[version] = 1;
  num_short_arc_groups_[version] = 0;
  num_short_arcs_[version] = 0;
  current_s_[version] = S_;
  arc_groups_[version] = 1;
  num_buckets_ = num_buckets;
  S_minus_one = S_ - 1;
  S_log_ = 0;
  while ((1UL << S_log_) < S_) {
    S_log_++;
  }
  assert((1UL << S_log_) == S_);
  for (size_t i = 1; i < num_buckets; i++) {
    NewBucket(version);
  }
  lh_n0 = S;
  uint64_t lh_2n0m1 = 2 * S - 1;
  lh_l = 0;
  lh_x = S_log_;
  while ((num_buckets >> lh_l) > lh_2n0m1) {
    lh_l++;
  }
  lh_n = num_buckets >> lh_l;
  lh_p = num_buckets - (lh_n << lh_l);

  for (uint32_t i = 0; i < num_buckets; i++) {
    int32_t ret = shm_alloc_page();
    if (ret == -1) {
      assert(0);
    }
    partition_map.partitions[i].pageNumber = ret;
    partition_map.partitions[i].pheader = shm_get_page_addr(ret);
    shm_clean_pages(ret);
    partition_map.numOfpartitions++;
  }
}

void *partition_header(uint32_t partitionNumber) { return partition_map.partitions[partitionNumber].pheader; }

uint32_t partition_pageNumber(uint32_t partitionNumber) { return partition_map.partitions[partitionNumber].pageNumber; }

static size_t HashToArc(uint64_t hash, Cbool version) {
  if (NumBuckets_v(version) < S_) {
    return ArcNum(NumBuckets_v(version), hash);
  }
  size_t arc_candidate = ArcNum((current_s_[version] + 1) * arc_groups_[version], hash);
  if (arc_candidate < num_short_arcs_[version]) {
    return arc_candidate;
  }
  arc_candidate = ArcNum(current_s_[version] * arc_groups_[version], hash) + num_short_arc_groups_[version];
  return arc_candidate;
}

static size_t ArcToBucket(size_t arc_num, Cbool version) {
  // The first S_ arcs are mapped onto themselves.
  if (arc_num < S_) {
    return arc_num;
  }
  uint64_t s_to_use = current_s_[version] + (num_short_arcs_[version] > arc_num);
  arc_num -= num_short_arc_groups_[version] & -(num_short_arcs_[version] <= arc_num);
#ifdef HARDCODE_DIVISION
  size_t arc_group = divf[s_to_use](arc_num);
#else
  size_t arc_group = arc_num / s_to_use;
#endif
  size_t position_in_group = arc_num - arc_group * s_to_use;
  size_t initial_groups_ = arc_groups_[version];
  if (s_to_use > S_) {
    initial_groups_ <<= 1;
    arc_group <<= 1;
    arc_group += (position_in_group >> S_log_);
    position_in_group &= S_minus_one;
  }
  size_t dist = __builtin_ctzll(arc_group);
  size_t new_ret = ((S_ + position_in_group) * initial_groups_ + arc_group) >> (dist + 1);
  return new_ret;
}

void get_last_short_group_parts(size_t *parts, size_t *count, Cbool version) {
  *count = 0;
  size_t max_arc_num = NumBuckets_v(version) - 1;
  // If finish a round
  if (num_short_arc_groups_[version] == 0) {
    size_t actual_count = current_s_[version];
    actual_count = actual_count == S_ ? ((S_ << 1) < (max_arc_num + 1) ? (S_ << 1) : max_arc_num + 1) : actual_count;
    for (; *count < actual_count; (*count)++) {
      parts[*count] = ArcToBucket(max_arc_num - *count, version);
    }
  } else {
    max_arc_num = num_short_arc_groups_[version] * (current_s_[version] + 1) - 1;
    for (; *count <= (current_s_[version]); (*count)++) {
      parts[*count] = ArcToBucket(max_arc_num - *count, version);
    }
  }
}

void get_first_long_group_parts(size_t *parts, size_t *count, Cbool version) {
  *count = current_s_[version];

  for (int i = current_s_[version] - 1; i >= 0; i--) {
    parts[i] = ArcToBucket(num_short_arcs_[version] + (current_s_[version] - 1 - i), version);
  }
}

void partitionMap_add(uint8_t *pheader, uint32_t pageNumber) {
  partition_map.partitions[partition_map.numOfpartitions].pheader = pheader;
  partition_map.partitions[partition_map.numOfpartitions].pageNumber = pageNumber;
  partition_map.numOfpartitions++;
}

EXTERN_END
