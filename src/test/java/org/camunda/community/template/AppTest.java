package org.camunda.community.template;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class AppTest {

  @Test
  public void testFoo() {
    final String actual = new App().foo();

    Assertions.assertThat(actual).isEqualTo("Hello World");
  }
}
