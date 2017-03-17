package com.alibaba.middleware.race.helper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.KV;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.ExtendBufferedReader;
import com.alibaba.middleware.race.utils.ExtendBufferedWriter;
import com.alibaba.middleware.race.utils.HashUtils;
import com.alibaba.middleware.race.utils.IOUtils;
import com.alibaba.middleware.race.utils.StringUtils;

/**
 * order的索引文件的创建方法 query 2 3 4的有2级索引，并保证1级索引的按buyerid或者goodid进行group
 * 
 * @author immortalCockRoach
 *
 */
public class OrderHashIndexCreator implements Runnable {
	private String hashId;

	private ExtendBufferedWriter[] offSetwriters;
	private Collection<String> files;
	private CountDownLatch latch;
	private int bucketSize;
	private boolean byteValueFormat;
	private HashSet<String> identitiesSet;
	private String queryPath;
	private IndexOperater operater;
	private int buildCount;
	private int mod;
	// 这个功能是要创建hashIndex文件，也就是中间文件，之后还需要合并
	public OrderHashIndexCreator(String hashId, String queryPath, ExtendBufferedWriter[] offsetWriters,
			Collection<String> files, int bUCKET_SIZE, int blockSize, CountDownLatch latch, String[] identities,
			boolean byteValueFormat, IndexOperater operater) {
		super();
		this.latch = latch;
		this.queryPath = queryPath;
		this.hashId = hashId;
		this.offSetwriters = offsetWriters;
		this.files = files;
		this.bucketSize = bUCKET_SIZE;
		this.identitiesSet = new HashSet<>(Arrays.asList(identities));
		this.byteValueFormat = byteValueFormat;
		this.operater = operater;
		this.buildCount = 0;
		this.mod = 524288;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		int fileIndex = 0;
		for (String orderFile : this.files) {
			Row kvMap = null;
			KV orderKV = null;
			int index;
			ExtendBufferedWriter offsetBw;
			// 记录当前行的偏移
			long offset = 0L;
			// 记录当前行的总长度
			int length = 0;
			try (ExtendBufferedReader reader = IOUtils.createReader(orderFile, CommonConstants.ORDERFILE_BLOCK_SIZE)) {
				String line = reader.readLine();
				while (line != null) {
					StringBuilder offSetMsg = new StringBuilder(50);
					// kvMap 存的每行的内容
					kvMap = StringUtils.createKVMapFromLineWithSet(line, CommonConstants.SPLITTER, this.identitiesSet);
					length = line.getBytes().length;

					// orderId一定存在且为long
					orderKV = kvMap.getKV(hashId);
					// bucketSize表征hash的份数，当length = 2^n的时候，hashcode & (len - 1) 就是取余
					index = HashUtils.indexFor(
							HashUtils.hashWithDistrub(
									hashId.equals("orderid") ? orderKV.getLongValue() : orderKV.valueAsString()),
							bucketSize);

					// 此处是rawValue还是longValue没区别
					offSetMsg.append(orderKV.valueAsString());
					offSetMsg.append(':');
					// 对于query2 加入createtime
					if (hashId.equals("buyerid")) {
						offSetMsg.append(kvMap.getKV("createtime").getLongValue());
						offSetMsg.append(' ');
					}
					offSetMsg.append(fileIndex);
					offSetMsg.append(' ');
					offSetMsg.append(offset);
					offSetMsg.append(' ');
					offSetMsg.append(length);
					offSetMsg.append('\n');
					// offset是源文件中的偏移量
					offset += (length + 1);
					buildCount++;
					// buildCount是哈希文件中的偏移量
					if ((buildCount & (mod - 1)) == 0) {
						System.out.println(hashId + "construct:" + buildCount);
					}

					offsetBw = offSetwriters[index];
					offsetBw.write(offSetMsg.toString());
					// offsetBw.newLine();

					line = reader.readLine();
				}
				fileIndex++;

			} catch (IOException e) {
				// 忽略
			}
		}
		// 需要对query 2 3的索引进行group合并入大文件并写入goodIndexMap和buyerIndexMap
		this.closeWriter();
		if (this.byteValueFormat) {
			if (hashId.equals("goodid")) {
				// 首先关闭流 然后对索引进行排序
				// 查询某个商品的交易信息,以及对某个商品的某个字段进行求和
				goodSeconderyIndex();
			} else {
				// 查询一段时间内某个买家的交易信息
				buyerSeconderyIndex();
			}
		}
		this.latch.countDown();
	}

	private void closeWriter() {
		try {

			for (ExtendBufferedWriter bw : offSetwriters) {
				bw.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void buyerSeconderyIndex() {
		// query3 4的文件按buyerid进行group
		// 这里的是初始化内存里面的内容，第一个参数是初始化table的空间，100%表示满了才扩容，默认是70%
		// 这是通过统计出来buyer有多少个算出来的
		// buyerMemoryIndexMap = new HashMap<>(8388608, 1f);
		// HashMap实际上底层用的数组表示的，这里的初始大小实际上是数组空间大小
		((BuyerIndexOperater) operater).createBuyerIndex();
		String orderedIndex = queryPath + File.separator + CommonConstants.INDEX_SUFFIX;
		Long offset = 0L;
		try (BufferedOutputStream orderIndexWriter = new BufferedOutputStream(new FileOutputStream(orderedIndex))) {
			for (int i = 0; i <= CommonConstants.QUERY2_ORDER_SPLIT_SIZE - 1; i++) {
				// 这里是按文件处理，每次读取一个中间文件，然后存到hashmap里面然后写到大文件里面
				// 因为100G分成1000个，每个也就100M，所以内存不会爆
				String indexFile = queryPath + File.separator + i + CommonConstants.INDEX_SUFFIX;
				// 对每个买家的记录进行group的map
				// System.out.println(indexFile);
				Map<String, List<byte[]>> groupedBuyerOrders = new HashMap<>(8192, 1f);
				try (ExtendBufferedReader orderIndexReader = IOUtils.createReader(indexFile,
						CommonConstants.ORDERFILE_BLOCK_SIZE)) {
					String line = orderIndexReader.readLine();
					while (line != null) {
						// buyerid定长20
						String buyerId = line.substring(0, 20);
						byte[] content = StringUtils.getBuyerByteArray(line.substring(21));
						if (groupedBuyerOrders.containsKey(buyerId)) {
							groupedBuyerOrders.get(buyerId).add(content);
						} else {
							List<byte[]> buyerOrdersList = new ArrayList<>(50);
							buyerOrdersList.add(content);
							groupedBuyerOrders.put(buyerId, buyerOrdersList);
						}
						line = orderIndexReader.readLine();
					}
				} catch (Exception e) {
					// TODO: handle exception
				}
				for (Map.Entry<String, List<byte[]>> e : groupedBuyerOrders.entrySet()) {
					// 因为metaTuple的大小是24bytes，所以800w buyer和400w good * 24也就400M，内存不会爆
					List<byte[]> list = e.getValue();
					MetaTuple buyerTuple = new MetaTuple(offset, list.size());
					// 内存二级索引 buyerid-tuple
					// buyerMemoryIndexMap.put(e.getKey(), buyerTuple);
					((BuyerIndexOperater) operater).addTupleToBuyerIndex(e.getKey(), buyerTuple);
					// 挨个写入有序的索引文件
					for (byte[] bytes : list) {
						orderIndexWriter.write(bytes);
						// buyer的有序索引中一个记录长度为24byte
						offset += 24L;
					}
				}
			}
		} catch (IOException e) {

		}
	}

	private void goodSeconderyIndex() {
		// query3 4的文件按goodid进行group
		// goodMemoryIndexMap = new HashMap<>(4194304, 1f);
		((GoodIndexOperater) operater).createGoodIndex();
		String orderedIndex = queryPath + File.separator + CommonConstants.INDEX_SUFFIX;
		Long offset = 0L;
		try (BufferedOutputStream orderIndexWriter = new BufferedOutputStream(new FileOutputStream(orderedIndex))) {
			for (int i = 0; i <= CommonConstants.QUERY3_ORDER_SPLIT_SIZE - 1; i++) {
				String indexFile = queryPath + File.separator + i + CommonConstants.INDEX_SUFFIX;
				// 对每个good的记录进行group的map
				// System.out.println(indexFile);
				Map<String, List<byte[]>> groupedGoodOrders = new HashMap<>(4096, 1f);
				try (ExtendBufferedReader orderIndexReader = IOUtils.createReader(indexFile,
						CommonConstants.ORDERFILE_BLOCK_SIZE)) {
					String line = orderIndexReader.readLine();
					while (line != null) {
						// goodId不定长 所以需要按:分割
						int p = line.indexOf(':');
						String goodId = line.substring(0, p);
						byte[] content = StringUtils.getGoodByteArray(line.substring(p + 1));
						if (groupedGoodOrders.containsKey(goodId)) {
							groupedGoodOrders.get(goodId).add(content);
						} else {
							List<byte[]> goodOrdersList = new ArrayList<>(100);
							goodOrdersList.add(content);
							groupedGoodOrders.put(goodId, goodOrdersList);
						}
						line = orderIndexReader.readLine();
					}
				} catch (Exception e) {
					// TODO: handle exception
				}
				for (Map.Entry<String, List<byte[]>> e : groupedGoodOrders.entrySet()) {

					List<byte[]> list = e.getValue();
					MetaTuple goodTuple = new MetaTuple(offset, list.size());
					// 内存二级索引 goodId-tuple
					((GoodIndexOperater) operater).addTupleToGoodIndex(e.getKey(), goodTuple);
					// 挨个写入有序的索引文件
					for (byte[] bytes : list) {
						orderIndexWriter.write(bytes);
						// good的有序索引中一个记录长度为16byte
						offset += 16L;
					}
				}
			}
		} catch (IOException e) {

		}
	}
}
