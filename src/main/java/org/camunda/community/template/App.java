package org.camunda.community.template;

public class App {

  public String foo() {
    return "Hello World";
  }

  public static void main(final String[] args) {
    System.out.println(new App().foo());
  }
}
