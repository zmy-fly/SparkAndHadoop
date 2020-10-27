package com.test;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONString;
import org.junit.Test;

import java.io.*;

/**
 * @author Bonze
 * @create 2020-10-13-11:53
 */
public class DemoTest {
	@Test
	public void test() throws JSONException, FileNotFoundException {


	}

}

class Employee{
	private String name;
	private int age;

	public int getAge() {
		return age;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "Employee{" +
				"name='" + name + '\'' +
				", age=" + age +
				'}';
	}

	public Employee(String name, int age) {
		this.name = name;
		this.age = age;
	}

	public Employee() {
	}
}