package com.vision.examples;

//Java Program to demonstrate the Use of Reflection

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

class ExampleClass {
	private String s;

	public ExampleClass() {
		s = "GeeksforGeeks";
	}

	public void method1() {
		// System.out.println("The string is " + s);
	}

	public void method2(int n) {
		// System.out.println("The number is " + n);
	}

	private void method3() {
		// System.out.println("Private method invoked");
	}
}

class ReflectionExample {

	public static void main(String args[]) throws Exception {

		ExampleClass obj = new ExampleClass();

		Class cls = obj.getClass();
		// System.out.println("The name of class is " + cls.getName());

		// Getting the constructor of the class through the object of the class
		Constructor constructor = cls.getConstructor();
		// System.out.println("The name of constructor is " + constructor.getName());

		// Display message only
		// System.out.println("The public methods of class are : ");

		// Getting methods of the class through the object of the class by using getMethods
		Method[] methods = cls.getMethods();

		// Printing method names
		/*for (Method method : methods)
			 System.out.println(method.getName());*/

		// Creates object of desired method by providing the method name as argument to the getDeclaredMethod()
		Method methodcall1 = cls.getDeclaredMethod("method1");

		// Invokes the method at runtime
		methodcall1.invoke(obj);

		// Creates object of desired method by providing the method name and parameter class as arguments to the getDeclaredMethod() method
		Method methodcall2 = cls.getDeclaredMethod("method2", int.class);

		// Invoking the method at runtime
		methodcall2.invoke(obj, 19);

		// Creates object of the desired field by providing the name of field as argument to the getDeclaredField() method
		Field field = cls.getDeclaredField("s");

		// Allows the object to access the field irrespective of the access specifier used with the field
		field.setAccessible(true);

		// Takes object and the new value to be assigned to the field as arguments
		field.set(obj, "JAVA");

		// Creates object of the desired method by providing the name of method as argument to the getDeclaredMethod() method
		Method methodcall3 = cls.getDeclaredMethod("method3");

		// Allows the object to access the method irrespective of the access specifier used with the method
		methodcall3.setAccessible(true);

		// Invoking the method at runtime
		methodcall3.invoke(obj);
	}
}