package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;
import scala.Tuple2;

import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Bonze
 * @create 2020-10-12-11:35
 */
public class JavaRDDTest {
	@Test
	public void test() {
//		SparkConf sparkConf = new SparkConf()
//				.setMaster("local[*]")
//				.setAppName("JavaSpark");
//		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//		JavaRDD<String> lines = jsc.textFile("input/score.txt", 1);
//		JavaRDD<String[]> splitRDD = lines.map(x -> x.split(","));
//
//		ArrayList<String> array = new ArrayList<>();
//
//		Stream<ArrayList<String>> stream = splitRDD.collect().stream().map(new java.util.function.Function<String[], ArrayList<String>>() {
//			ArrayList<String> arrayList = new ArrayList<>();
//			double sum = 0;
//			double aver = 0;
//			@Override
//			public ArrayList<String> apply(String[] strings) {
//				for (int i = 0; i < strings.length; i++) {
////					System.out.println(strings[i]);
//					if (i == 0 || i == 1) {
//						arrayList.add(i,strings[i]);
//						continue;
//					}
//					sum += Double.parseDouble(strings[i]);
//				}
//				int count = strings.length - 2;
//				aver = sum / count;
//				arrayList.add(sum);
//				return arrayList;
//			}
//		});
//
//		Stream<String> StringStream = stream.flatMap(new java.util.function.Function<ArrayList<String>, Stream<String>>() {
//			@Override
//			public Stream<String> apply(ArrayList<String> Strings) {
//				return Strings.stream();
//			}
//		});

//		ArrayBuffer<String> arrayBuffer = new ArrayBuffer<>();
//		ArrayList<String> arrayList = new ArrayList<>();
//		for(int i = 1; i <= 5; i++) {
//			arrayBuffer += arrayList.get(i);
//		}
//
//		JavaRDD<String> parallelize = jsc.parallelize(StringStream.collect(Collectors.toList()));
//
//		JavaPairRDD<String, Iterable<ArrayList<String>>> StringIterableJavaPairRDD = parallelize.groupBy(new Function<ArrayList<String>, String>() {
//			@Override
//			public String call(ArrayList<String> Strings) throws Exception {
//				return Strings.get(0);
//			}
//		});

//		StringIterableJavaPairRDD.collect().forEach(new Consumer<Tuple2<String, Iterable<ArrayList<String>>>>() {
//			@Override
//			public void accept(Tuple2<String, Iterable<ArrayList<String>>> StringIterableTuple2) {
//				System.out.println(StringIterableTuple2._1);
//				StringIterableTuple2._2().forEach(System.out::println);
//			}
//		});


//		stream.forEach(k -> {
//			k.forEach(System.out::println);
//		});

//		stream.forEach(new Consumer<ArrayList<String>>() {
//			@Override
//			public void accept(ArrayList<String> Strings) {
//				for(String String : Strings){
//					System.out.println(String);
//				}
//			}
//		});


//		JavaRDD<String> map = splitedRDD.map(new Function<String[], String>() {
//			@Override
//			public String call(String[] s) throws Exception {
//				double sum = 0;
//				int count = 0;
//				double aver = 0;
//				for (int i = 2; i <= s.length - 1; i++) {
//					sum += Double.parseDouble(s[1]);
//				}
//				aver = sum / count;
//				String s1 = s[0] + "," + s[1] + "," + aver;
//				return s1;
//			}
//		});
//		map.foreach(System.out::println);


	}

	@Test
	public void test2(){
		SparkConf sparkConf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("JavaSpark");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = jsc.textFile("input/score.txt", 1);
		JavaRDD<String[]> splitRDD = lines.map(x -> x.split(","));

		Stream<String[]> stream = splitRDD.collect().stream().map(new java.util.function.Function<String[], String[]>() {
			String[] Strings = new String[3];

			@Override
			public String[] apply(String[] strings) {
				double sum = 0;
				double aver = 0;
				for (int i = 0; i < strings.length; i++) {
					if (i == 0 || i == 1) {
						Strings[i] = strings[i];
						continue;
					}
					sum += Double.parseDouble(strings[i]);
				}
				int count = strings.length - 2;
				aver = sum / count;
				String aver1 = String.format("%.1f", aver);
				Strings[2] = aver1;
				return Strings;
			}
		});

		JavaRDD<String[]> parallelize = jsc.parallelize(stream.collect(Collectors.toList()));

		JavaPairRDD<String, Iterable<String[]>> StringIterableJavaPairRDD = parallelize.groupBy(new Function<String[], String>() {
			@Override
			public String call(String[] Strings) throws Exception {
				return Strings[0];
			}
		});

		StringIterableJavaPairRDD.collect().forEach(new Consumer<Tuple2<String, Iterable<String[]>>>() {
			@Override
			public void accept(Tuple2<String, Iterable<String[]>> StringIterableTuple2) {
				System.out.println(StringIterableTuple2._1);
				StringIterableTuple2._2.forEach(k -> {
					System.out.println(k[1] + "得分为" + k[2]);
				});
			}
		});

//		StringIterableJavaPairRDD.collect().forEach(new Consumer<Tuple2<String, Iterable<ArrayList<String>>>>() {
//			@Override
//			public void accept(Tuple2<String, Iterable<ArrayList<String>>> StringIterableTuple2) {
//				System.out.println(StringIterableTuple2._1);
//				StringIterableTuple2._2().forEach(System.out::println);
//			}
//		});

	}
	@Test
	public void test4(){
//		SparkConf sparkConf = new SparkConf()
//				.setMaster("local[*]")
//				.setAppName("JavaSpark");
//		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//		JavaRDD<String> lines = jsc.textFile("input/score.txt", 1);
//		JavaRDD<String[]> splitRDD = lines.map(x -> x.split(","));
//
//		splitRDD.flatMap(new FlatMapFunction<String[], Object>() {
//			@Override
//			public Iterator<Object> call(String[] strings) throws Exception {
//
//			}
//		});
//
//
//
//		Stream<String[]> stream = splitRDD.collect().stream().map(new java.util.function.Function<String[], String[]>() {
//			String[] Strings = new String[3];
//
//			@Override
//			public String[] apply(String[] strings) {
//				double sum = 0;
//				double aver = 0;
//				for (int i = 0; i < strings.length; i++) {
//					if (i == 0 || i == 1) {
//						Strings[i] = strings[i];
//						continue;
//					}
//					sum += Double.parseDouble(strings[i]);
//				}
//				int count = strings.length - 2;
//				aver = sum / count;
//				String aver1 = String.format("%.1f", aver);
//				Strings[2] = aver1;
//				return Strings;
//			}
//		});
//
//		JavaRDD<String[]> parallelize = jsc.parallelize(stream.collect(Collectors.toList()));
//
//		JavaPairRDD<String, Iterable<String[]>> StringIterableJavaPairRDD = parallelize.groupBy(new Function<String[], String>() {
//			@Override
//			public String call(String[] Strings) throws Exception {
//				return Strings[0];
//			}
//		});
//
//		StringIterableJavaPairRDD.collect().forEach(new Consumer<Tuple2<String, Iterable<String[]>>>() {
//			@Override
//			public void accept(Tuple2<String, Iterable<String[]>> StringIterableTuple2) {
//				System.out.println(StringIterableTuple2._1);
//				StringIterableTuple2._2.forEach(k -> {
//					System.out.println(k[1] + "得分为" + k[2]);
//				});
//			}
//		});

//		StringIterableJavaPairRDD.collect().forEach(new Consumer<Tuple2<String, Iterable<ArrayList<String>>>>() {
//			@Override
//			public void accept(Tuple2<String, Iterable<ArrayList<String>>> StringIterableTuple2) {
//				System.out.println(StringIterableTuple2._1);
//				StringIterableTuple2._2().forEach(System.out::println);
//			}
//		});

	}

	@Test
	public void test3(){
		SparkConf sparkConf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("JavaSpark");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = jsc.textFile("input/score.txt", 1);
		JavaRDD<String[]> splitRDD = lines.map(x -> x.split(","));

		Stream<String[]> stream = splitRDD.collect().stream().map(new java.util.function.Function<String[], String[]>() {
			String[] Strings = new String[3];

			@Override
			public String[] apply(String[] strings) {
				double sum = 0;
				double aver = 0;
				for (int i = 0; i < strings.length; i++) {
					if (i == 0 || i == 1) {
						Strings[i] = strings[i];
						continue;
					}
					sum += Double.parseDouble(strings[i]);
				}
				int count = strings.length - 2;
				aver = sum / count;
				String aver1 = String.format("%.1f", aver);
				Strings[2] = aver1;
				return Strings;
			}
		});

		JavaRDD<String[]> parallelize = jsc.parallelize(stream.collect(Collectors.toList()));

		JavaPairRDD<String, Iterable<String[]>> StringIterableJavaPairRDD = parallelize.groupBy(new Function<String[], String>() {
			@Override
			public String call(String[] Strings) throws Exception {
				return Strings[0];
			}
		});

		StringIterableJavaPairRDD.collect().forEach(new Consumer<Tuple2<String, Iterable<String[]>>>() {
			@Override
			public void accept(Tuple2<String, Iterable<String[]>> StringIterableTuple2) {
				System.out.println(StringIterableTuple2._1);
				StringIterableTuple2._2.forEach(k -> {
					System.out.println(k[1] + "得分为" + k[2]);
				});
			}
		});
	}


}
