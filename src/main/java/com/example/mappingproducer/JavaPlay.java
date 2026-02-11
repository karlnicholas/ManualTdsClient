package com.example.mappingproducer;

/**
 * The Main class implements an application that reads lines from the standard input
 * and prints them to the standard output.
 */
public class JavaPlay {
  /**
   * Iterate through each line of input.
   */
  public static void main(String[] args) throws InterruptedException {
    new JavaPlay().run2();
  }

  private void run2() {

    System.out.println("Main Thread Start: " + Thread.currentThread().getName());

    MappingProducer.just(1)
        .map(i -> i + 1)
        .map(i -> i + 1)
        .subscribe(i -> System.out.println("Value: " + i));

    System.out.println("Main Thread End");

  }

//  private void run() throws InterruptedException {
//    IntegerPublisher publisher = new IntegerPublisher();
//
//    MappingProducer.from(publisher).map(i->i+1).map(i->i+1).subscribe(System.out::println);
//
//    }

}