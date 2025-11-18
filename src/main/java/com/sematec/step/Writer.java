package com.sematec.step;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

public class Writer implements ItemWriter<String> {

	@Override
	public void write(Chunk<? extends String> chunk) throws Exception {
		for (String msg : chunk) {
			System.out.println("Writing the data " + msg);
		}
	}

}