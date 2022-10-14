#include "hashtable.hpp"

HashTable::HashTable(MemPool* mempool){
	is_setting_ = 0;
	is_flexibling_ = 0;
	current_version_ = 0;
	table_block_num_ = mempool->getAvailableNum();
	assert(table_block_num_);

	round_hash_ = RoundHash(table_block_num_, 8);

	for (uint32_t i = 0; i < table_block_num_; i++) {
		int32_t ret = mempool->allocBlock();
		assert(ret + 1); 
		table_blocks_[i]->block_id = (uint32_t)ret;
		table_blocks_[i]->block_ptr = mempool->get_block_ptr(ret);
		mempool->cleanBlock(ret);
	}
}

HashTable::~HashTable(){
  /* …… */
}

void *HashTable::get_block_ptr(uint32_t tableIndex){
	return table_blocks_[tableIndex]->block_ptr;
}

uint32_t HashTable::get_block_id(uint32_t tableIndex){
	return table_blocks_[tableIndex]->block_id;
}

void HashTable::AddBlock(uint8_t *pheader, uint32_t block_id){
	table_blocks_[table_block_num_]->block_ptr = pheader;
	table_blocks_[table_block_num_]->block_id = block_id;
	table_block_num_++;
}

void HashTable::RemoveBlock(){
  table_blocks_[table_block_num_-1]->block_ptr = NULL;
  table_blocks_[table_block_num_-1]->block_id = -1;
  table_block_num_--;
}
  /* naive transplant */
void HashTable::ShrinkTable(size_t blockNum){
	size_t count;
	size_t parts[round_hash_.S_ << 1];
	/* once in a cycle*/
	while (blockNum--){
		round_hash_.calculate_parts_to_remove(parts, &count);//current_version_???
		round_hash_.DelBucket();

		while (1){
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U)) break;
		}
		*(volatile uint32_t *)&(is_flexibling_) = 1U;
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);

		redistribute_last_short_group(parts, count);
		table_block_num_--;

		while (1){
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U)) break;
		}
		uint64_t v = (uint64_t)(!current_version_) << 32;
		*(volatile uint64_t *)&(is_flexibling_) = v;
		/* clean is_flexibling and flip current_version_ in a single step */
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);
	}
}

void HashTable::ExpandTable(size_t blockNum){
	size_t count;
	size_t parts[round_hash_.S_ << 1];
	/* once in a cycle*/
	while (blockNum--){
		calculate_parts_to_add(parts, &count, current_version_);
		NewBucket_v(current_version_);

		while (1){
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U)) break;
		}
		*(volatile uint32_t *)&(is_flexibling_) = 1U;
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);

		redistribute_first_long_group(parts, count);
		table_block_num_++;

		while (1){
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U)) break;
		}
		uint64_t v = (uint64_t)(!current_version_) << 32;
		*(volatile uint64_t *)&(is_flexibling_) = v;
		/* clean is_flexibling and flip current_version_ in a single step */
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);
	}  
}