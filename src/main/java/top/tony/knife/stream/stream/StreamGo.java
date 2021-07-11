package top.tony.knife.stream.stream;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;

public class StreamGo {

    private final URL url;
    private final InputStream in;

    private volatile OkHttpClient client;

    /**
     * read from input stream
     *
     * @param in
     */
    public StreamGo(InputStream in) {
        this.url = null;
        this.in = in;
    }

    /**
     * read from url
     *
     * @param url
     */
    public StreamGo(URL url) {
        this.url = url;
        this.in = null;
    }

    public static StreamGo fromFile(String filepath) {
        try {
            InputStream in;
            if (filepath.startsWith("classpath://")) {
                String p = filepath.split("classpath://")[1];
                in = Thread.currentThread().getContextClassLoader().getResourceAsStream(p);
            } else {
                in = new FileInputStream(filepath);
            }
            return new StreamGo(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static StreamGo fromHttp(String url) {
        try {
            return new StreamGo(new URL(url));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public static StreamGo fromHttp(URL url) {
        return new StreamGo(url);
    }

    public static StreamGo from(InputStream in) {
        return new StreamGo(in);
    }

    public void line(LineAction act) {
        if (readFromUrl()) {
           OkHttpClient cli = okHttpClient();
            Request req = new Request.Builder()
                    .url(url)
                    .build();
            try (Response resp = cli.newCall(req).execute()) {
                InputStream in = Objects.requireNonNull(resp.body()).byteStream();
                doReadLine(act, in);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (readFromInputStream()) {
            doReadLine(act, in);
        } else {
            throw new IllegalArgumentException("invalid input, please create valid StreamGo by from() or fromUrl() or fromFile().");
        }
    }

    private void doReadLine(LineAction act, InputStream in) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                act.action(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private OkHttpClient okHttpClient() {
        if (client == null) {
            client = new OkHttpClient();
        }
        return client;
    }

    private boolean readFromInputStream() {
        return in != null;
    }

    private boolean readFromUrl() {
        return url != null;
    }

    public interface LineAction {
        void action(String line);
    }
}
