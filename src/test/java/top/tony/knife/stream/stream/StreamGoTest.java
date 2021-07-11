package top.tony.knife.stream.stream;

import org.junit.Test;

public class StreamGoTest {

    @Test
    public void fromFile() {
        StreamGo.fromFile("classpath://demo.txt")
                .line(System.out::println);
    }

    @Test
    public void fromUrl() {
        StreamGo.fromHttp("https://www.baidu.com")
                .line(System.out::println);
    }
}