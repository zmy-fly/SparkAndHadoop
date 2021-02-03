package com.school.spark;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 * Ctrl+Shift+t 搜索源文件快捷键
 * @author Bonze
 * @create 2020-09-23-9:55
 */
public class TestDemo {
	@Test
	public void test() {

		Consumer<String> con = str -> {
			System.out.println(str);
		};

		List<String> list = new ArrayList<>();
		Stream<String> stream = Stream.of("a", "b", "c");
//		stream.filter();
	}
	private void add(int x, int y){
		int sum = x + y;
	}
}



