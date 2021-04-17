package com.springbatch.batch;

import org.springframework.batch.item.ItemProcessor;

import com.springbatch.model.Player;

public class PlayerItemProcessor implements ItemProcessor<Player, Player> {

	@Override
	public Player process(Player item) throws Exception {
		
		return item;
	}

}
