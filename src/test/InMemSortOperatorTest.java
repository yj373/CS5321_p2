package test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.Test;

import data.Tuple;

class InMemSortOperatorTest {

	@Test
	void test() {
		long[] data1 = {1,4,4,4};
		long[] data2 = {1,4,3,3};
		long[] data3 = {6,4,3,1};
		long[] data4 = {2,5,5,5};
		long[] data5 = {2,2,5,5};
		Map<String, Integer> schema1 = new HashMap<String, Integer>();
		schema1.put("S.A", 0);
		schema1.put("S.B", 1);
		schema1.put("R.G", 2);
		schema1.put("R.H", 3);
		Tuple t1 = new Tuple(data1, schema1);
		Tuple t2 = new Tuple(data2, schema1);
		Tuple t3 = new Tuple(data3, schema1);
		Tuple t4 = new Tuple(data4, schema1);
		Tuple t5 = new Tuple(data5, schema1);
		Tuple[] arr = {t1,t2,t3,t4,t5, null};
		LinkedList<String> orderList = new LinkedList<String>();
		orderList.add("R.H");
		orderList.add("S.B");
		//orderList.add("S.A");
		
		
	}

}
