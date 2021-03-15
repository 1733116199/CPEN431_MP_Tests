package cpen431.mp.KeyValue;

public class KVSResultField implements Comparable<KVSResultField> {
	public String key;
	public String value;

	public KVSResultField(String key, String value) {
		this.key = key.replace(",", "");
		this.value = value.replace(",", "");
	}

	public KVSResultField(String key, Double value) {
		this.key = key.replace(",", "");
		this.value = value.toString();
	}

	@Override
	public int compareTo(KVSResultField field) {
		return this.key.compareTo(field.key);
	}
}
