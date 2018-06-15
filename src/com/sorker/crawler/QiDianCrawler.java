package com.sorker.crawler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class QiDianCrawler {
	// hdfs 基础配置
	private static Configuration conf = new Configuration();
	private static String BASE_PATH = "hdfs://192.168.198.129:9000/qidian_dig2/";
	private static Path basePath = new Path(BASE_PATH);
	private static FileSystem fs;

	private static List<String> allWaitUrls = new ArrayList<>(); 	// 先进先出列表
	private static Set<String> allOverUrls = new HashSet<>(); 		// 已处理列表
	private static Map<String, Integer> UrlDepth = new HashMap<>(); // 当前链接深度
	
//	private static Set<String> typesSets = new HashSet<>(); 		
//	private static Map<String, Integer> typesMaps = new HashMap<>(); 

	// head数组，随机
	private static String[] UseAgent = {
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299",
			"Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.162 Mobile Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
			"Mozilla/5.0 (Windows NT 6.1; rv:52.0) Gecko/20100101 Firefox/52.0",
			"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36" };

	private static final int MAXDEPTH = 15; // 最大深度
	private static final int threadNum = 50;// 线程数量

	private static int count = 10; // 剩余处理数量
	private static int bookNum = 1; // 已添加的数量
//	private static int NullScore = 0;
	// 准备一个变量，记录现在有多少线程空闲，如果10个线程都空闲，就表示整个程序结束。
	private static int idleThread = 0; // 线程空闲数

	// 声明一个同步的操作对象,该对象用来让线程睡眠以及进行唤醒.
	private static Object obj = new Object();

	public static void main(String[] args) {

//		typesSets.add("游戏_虚拟网游");
//		typesMaps.put("游戏_虚拟网游", 1);
		
		String seedUrl = "https://www.qidian.com";
		allWaitUrls.add(seedUrl);
		allOverUrls.add(seedUrl);
		UrlDepth.put(seedUrl, 1);

		for (int i = 0; i < threadNum; i++) {
			new QiDianCrawler().new MyThread().start();
		}
	}
	/**
	 * 
	 * @Title addUrls
	 * @Desripection TODO
	 * @throws 
	 * @param inputUrl
	 *
	 */

	private static void addUrls(String inputUrl) {
		Random random = new Random();
		int useAgentNum = random.nextInt(5);// 随机选取useagent
		--count; // 每处理一个地址就减少一个处理数量
		int depth = UrlDepth.get(inputUrl); // 获取扫描深度

		// 打印当前处理内容
		System.out.println(Thread.currentThread().getName() + "，深度"+ depth +"，正在使用" + useAgentNum + "处理：" + inputUrl + "--" + count);

		try {
			// 打开链接
			URL url = new URL(inputUrl);
			URLConnection conn = url.openConnection();
			conn.setRequestProperty("user-agent", UseAgent[useAgentNum]);
			conn.setConnectTimeout(1000);
			InputStream is = conn.getInputStream();
			// io流存储到builder
			BufferedReader reader = new BufferedReader(new InputStreamReader(is, "utf-8"));
			StringBuilder builder = new StringBuilder();
			String line = null;
			// 如果有内容则存储
			while ((line = reader.readLine()) != null) {
				builder.append(line);
			}
			reader.close();

			// 用Jsoup类处理html文档
			Document doc = Jsoup.parse(builder.toString());
			docDeal(inputUrl, doc);

			// 获取该页a标签
			if (depth < MAXDEPTH) {
				Elements allLinks = doc.getElementsByTag("a");
				for (Element link : allLinks) {
					// 得到a标签中的href地址
					String href = link.attr("href");
					// 过滤无用链接
					if (!href.contains("javascript:") && !href.contains("/reviews") && !href.contains("/comment")&& !href.contains("/thread") && !href.contains("/QDReader") && !href.contains("/catalog")&& !href.contains("/forum") && !href.contains("/fans") && !href.contains("/index") && !href.contains("/about") && !href.contains("#item")  && !href.contains("/all") 
//							&& !href.contains("hiddenField=2")  
							&& !href.contains("hiddenField=1") && !href.matches(".*/\\d+/0")) {
						// 把要处理的链接加入列队
						if (!href.startsWith("http:") && !href.startsWith("https:") && !href.startsWith("//")) {
							// 同步处理
							synchronized (obj) {
								// 去重
								href = "https://m.qidian.com" + href;
								addIntoLSM(href, depth);
							}
						} else {
							// 处理的地址包含“手机起点”
							if (href.contains("m.qidian.com")) {
								// 同步处理
								synchronized (obj) {
									// 去重
									href = href.substring(href.indexOf("//") + 2 );
									href = "https://" + href;
									addIntoLSM(href, depth);
								}
							} else if (href.contains("book.qidian.com/info")) {
								// 同步处理
								synchronized (obj) {
									// 去重
									href = href.substring(href.indexOf("/info") + 5);
									href = "https://m.qidian.com/book" + href;
									addIntoLSM(href, depth);
								}

							} else if (href.contains("www.qidian.com")) {
								// 同步处理
								synchronized (obj) {
									// 去重
									href = href.substring(href.indexOf("//") + 2 );
									href = "https://" + href;
									addIntoLSM(href, depth);
								}
							}
						}
					}
					// 唤醒线程
					synchronized (obj) {
						obj.notify();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @Title docDeal
	 * @Desripection //处理html文本提出所需字段
	 * @throws @param
	 *             inputUrl
	 * @param doc
	 * @throws Exception
	 *
	 */
	private static void docDeal(String inputUrl, Document doc) throws Exception {
		// 处理的字段
		String bookName = null, bookScore = null, bookNumber = null;
		String bookTags = "", bookTypes = "";
		// 如果是想要处理的页面则提取内容
		if (inputUrl.matches(".*book/\\d+")) {
			// 提取标题
			bookName = doc.title();
			bookName = bookName.substring(0, bookName.indexOf("_"));
			// 提取标签
			Elements allTags = doc.getElementsByTag("a");
			for (Element tags : allTags) {
				String tag = tags.attr("href");
				String sysbom = "/search?kw=&tag=";
				if (tag.contains(sysbom)) {
					String length = tag.substring(sysbom.length());
					bookTags += "||" + length + ":1";
				}
			}
			// 提取评分
			Elements inScoreClass = doc.getElementsByClass("book-score");
			for (Element scoreC : inScoreClass) {
				String Score = scoreC.text();
				if (Score.toString().length() > 9) {
					bookScore = Score.substring(0, Score.indexOf(" "));
				}
			}
			// 提取字数与分类
			Elements inNumbersTypesClass = doc.getElementsByClass("book-meta");
			String[] NumberTypes = new String[3];
			int n = 0;
			for (Element NumberTypesC : inNumbersTypesClass) {
				String NumberType = NumberTypesC.text();
				NumberTypes[n] = NumberType.toString();
				n++;
			}
			// 分类
			String[] types = NumberTypes[0].split("/");
			bookTypes = types[0] + "_" + types[1];
//			if (!typesSets.contains(bookTypes)) {
//				typesSets.add(bookTypes);
//				typesMaps.put(bookTypes, 1);
//			}else {
//				int typeNum = typesMaps.get(bookTypes) + 1; 
//				typesMaps.remove(bookTypes);
//				typesMaps.put(bookTypes, typeNum);
//			}
			// 字数
			if(NumberTypes[1].contains("万字")) {
				String[] numbers = NumberTypes[1].split("万字");
				bookNumber = numbers[0];
			}else if (NumberTypes[1].contains("字")) {
				String[] numbers = NumberTypes[1].split("字");
				Double num = Double.parseDouble(numbers[0]) / 10000.0000;
				bookNumber = num + "";
//				System.out.println(bookNumber);
			}
			
		}
		// System.out.println("已添加：" + bookNum++);
		// 把所得的内容存入文档
		if (bookName != null) {
			 upTextHDFS(bookName, bookTypes, bookNumber, bookScore, bookTags);
		}
	}

	/**
	 * 
	 * @Title upTextHDFS
	 * @Desripection 上传数据到hdfs
	 * @throws @param
	 *             bookName
	 * @param bookTypes
	 * @param bookNumber
	 * @param bookScore
	 * @param bookTags
	 * @throws IOException
	 *
	 */
	private static void upTextHDFS(String bookName, String bookTypes, String bookNumber, String bookScore,
			String bookTags) throws IOException {
		// 写入hdfs
		// 所要写出的文件路径和名称
		fs = basePath.getFileSystem(conf);
		Path path = new Path(BASE_PATH + bookName + ".txt");
		// 通过文件系统取得一个IO流
		FSDataOutputStream os = fs.create(path);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
		writer.write(bookTypes + ":" + bookNumber + "||" + bookTypes + "Sroce:" + bookScore + bookTags);
		// System.out.println(bookTypes + ":" + bookNumber + "、" + bookTypes + ":"
		// +bookScore + "、" + bookTags);
		writer.close();
		System.out.println("已添加：" + bookNum++);
//		if (bookScore.equals("null")) {
//			NullScore++;
//		}
		// 写入本地文本
		// PrintWriter writer = new PrintWriter(new File("D:/Users/linke/Desktop/qidian/" + (bookNum-1) + ".txt"));
		// writer.println(bookName);
		// writer.println(bookTypes);
		// writer.println(bookNumber);
		// writer.println(bookScore);
		// writer.println(bookTags);
		// writer.close();
	}

	/**
	 * 
	 * @ProjectName news3_title_search
	 * @Description 多线程类
	 * @ClassName MyThread
	 * @author linke
	 * @data 2018年4月20日 下午7:04:59
	 * @Version 1.0.0
	 *
	 */
	class MyThread extends Thread {
		@Override
		public void run() {
			while (true) {
				String inputUrl = null;

				synchronized (obj) {
					if (allWaitUrls.size() > 0) {
						// 获取处理的链接
						inputUrl = allWaitUrls.get(0);
						// 去库
						allWaitUrls.remove(0);
					}
				}

				if (inputUrl != null) {
					addUrls(inputUrl);
				} else {
					try {
						idleThread++;
						if (idleThread == threadNum) {							
//							for (Map.Entry<String, Integer> entrySet : typesMaps.entrySet()) {
//								System.out.println(entrySet.getKey() + entrySet.getValue());
//							}
							System.exit(0);
						}
						synchronized (obj) {
							obj.wait();

						}
						idleThread--;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	/**
	 * 
	 * @Title addInLSM
	 * @Desripection TODO
	 * @throws 
	 * @param href
	 * @param depth
	 *
	 */
	private static void addIntoLSM(String href,int depth) {
		if (!allOverUrls.contains(href)) {
			count++; // 增加所要处理的数量
			allWaitUrls.add(href);
			allOverUrls.add(href);
			UrlDepth.put(href, depth + 1);
		}
	}
}
