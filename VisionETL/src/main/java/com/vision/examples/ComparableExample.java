package com.vision.examples;

import java.util.ArrayList;
import java.util.Collections;

public class ComparableExample {

	public static void main(String[] args) {
		ArrayList<Node> al = new ArrayList<Node>();
		al.add(new Node(101, "Read", 2));
		al.add(new Node(106, "Concat", 1));
		al.add(new Node(105, "substract", 1));
		al.add(new Node(107, "Write", 0));

		Collections.sort(al);
		for (Node st : al) {
			// System.out.println(st.nodeId + " " + st.name + " " + st.executionOrder);
		}
	}

}

class Node implements Comparable<Node> {
	int nodeId;
	String name;
	int executionOrder;

	Node(int nodeId, String name, int executionOrder) {
		this.nodeId = nodeId;
		this.name = name;
		this.executionOrder = executionOrder;
	}

	public int compareTo(Node st) {
		if (executionOrder == st.executionOrder)
			return 0;
		else if (executionOrder < st.executionOrder)
			return 1;
		else
			return -1;
	}
}