package operators;

import java.util.Comparator;
import java.util.LinkedList;

import data.Tuple;

public class TupleComparator_test implements Comparator<Tuple> {
	private LinkedList<String> compareOrder;
	public TupleComparator_test (LinkedList<String> attrList) {
		this.compareOrder = attrList;
	}

	@Override
	public int compare(Tuple o1, Tuple o2) {
		if(compareOrder.size() == 0) return 0;
		if (o1 == null && o2 == null) {
			return 0;
		}
		if (o1 == null) {
			return 1;
		}
		if (o2 == null) {
			return -1;
		}
		for (String collumn: compareOrder) {
			int index = o1.getSchema().get(collumn);
			if (o1.getData()[index]<o2.getData()[index]) {
				return -1;
			}
			if (o1.getData()[index]>o2.getData()[index]) {
				return 1;
			}
		}
		for (int i = 0; i < o1.getSchema().size(); i++) {
			if (o1.getData()[i] < o2.getData()[i]) {
				return -1;
			} 
			if (o1.getData()[i] > o2.getData()[i]){
				return 1;
			} 
		}
		return 0;
	}

}
