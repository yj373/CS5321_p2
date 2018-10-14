package data;

public class TablePair {
    private String firstTable;
    private String secondTable;
    
    public TablePair(String str1, String str2) {
    	firstTable = str1;
    	secondTable = str2;
    }
    
    public String first() {
    	return firstTable;
    }
    
    public String second() {
    	return secondTable;
    }
    
    @Override
	public boolean equals(Object e) {
    	if (e.getClass() != this.getClass()) {
    		return false;
    	}
    	TablePair o = (TablePair) e;
    	if (this.first().equals(o.first()) && this.second().equals(o.second())) {
    		return true;
    	}
    	if (this.first().equals(o.second()) && this.second().equals(o.first())) {
    		return true;
    	}
    	return false;
    }
    
    @Override
    public int hashCode() {
    	return (firstTable.hashCode() + secondTable.hashCode()) % 127;
    }
}
