/** imageDownloader (Google).
 *
 * Author: Roberto Gonzalez <roberto.gonzalez@neclab.eu>
 *
 * Copyright (c) 2017, NEC Europe Ltd., NEC Corporation. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THIS HEADER MAY NOT BE EXTRACTED OR MODIFIED IN ANY WAY.
 */
package robegs.webCategories;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.Normalizer;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.regex.Pattern;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author robegs
 */
public class MetaDataReducer extends Reducer<Text, Text, Text, Text> {

    public static final Log LOG = LogFactory.getLog(MetaDataReducer.class);
    public static final Pattern patCOMMA = Pattern.compile(",");

    public static final String UA = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0";
    public static final String CFCOOKIE_NAME = "__cfduid";
    public static final String CFCOOKIE_VALUE = "dd57c1132d9ef915b2595fe7df3639c991470659991";
    public static final String REFERRER[] = {"https://www.google.com", "https://www.google.de", "https://www.bing.com", "https://duckduckgo.com/"};
    public static final Text ERROR = new Text("ERROR");
    public static final Text TIMEOUT = new Text("TIMEOUT");

    public static final Pattern patternDIACRITICAL = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String url = key.toString();
        if (!url.startsWith("http")) {
            url = "http://" + url;
        }

        System.out.println("Downloading: " +url);
        try {
            if (!context.getConfiguration().get("conf.host", "").equals("robegs-laptop.office.hd")) {
                System.setProperty("http.proxyHost", "192.168.0.1");
                System.setProperty("http.proxyPort", "8123");
                System.setProperty("https.proxyHost", "192.168.0.1");
                System.setProperty("https.proxyPort", "8123");
                System.setProperty("socksProxyHost", "192.168.0.1");
                System.setProperty("socksProxyPort", "8123");
                disableSSLCertificateChecking();
            }
            // This will get input data from the server
            InputStream inputStream = null;

            // This will read the data from the server;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                URL url_img = new URL(url);
                // This socket type will allow to set user_agent
                URLConnection con = url_img.openConnection();
                // Setting the user agent
                con.setRequestProperty("Referer", REFERRER[new Random().nextInt(REFERRER.length)]);
                con.setRequestProperty("User-Agent", UA);
                con.setRequestProperty("Cookie", CFCOOKIE_NAME+"="+CFCOOKIE_VALUE);

                con.setConnectTimeout(30*1000);
                // Requesting input data from server
                inputStream = con.getInputStream();
                // Limiting byte written to file per loop
                byte[] buffer = new byte[2048];
                // Increments file size
                int length;
                // Looping until server finishes
                while ((length = inputStream.read(buffer)) != -1) {
                    // Writing data
                    out.write(buffer, 0, length);
                }
                byte[] response = out.toByteArray();

                for (Text val : values) {
                    Path pt = FileOutputFormat.getOutputPath(context);
                    pt = pt.suffix("/" + val.toString());
                    FileSystem fs = FileSystem.get(new Configuration());
                    FSDataOutputStream outImg = fs.create(pt);
                    outImg.write(response);
                    outImg.close();
                }

            } catch (IOException e) {
                System.out.println("IOError");
                System.out.println(e.getMessage());
                
            }

        } catch (Exception e) {
            System.out.println("Other error:" + url);
            e.printStackTrace();
            System.out.println(e.getLocalizedMessage());
            System.out.println(e.getMessage());
        }
    }

    MultipleOutputs mos = null;

    public void close() throws IOException {
        // TODO Auto-generated method stub
        mos.close();
    }

    public void configure(JobConf job) {
        mos = new MultipleOutputs(job);
    }

    public static String stripAccents(String s) {
        s = Normalizer.normalize(s, Normalizer.Form.NFD);

        //s = s.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
        s = patternDIACRITICAL.matcher(s).replaceAll("").replace("\n", "").replace("\r", "");
        return s;
    }

    /**
     * Disables the SSL certificate checking for new instances of
     * {@link HttpsURLConnection} This has been created to aid testing on a
     * local box, not for use on production.
     */
    private static void disableSSLCertificateChecking() {
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            @Override
            public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                // Not implemented
            }

            @Override
            public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                // Not implemented
            }
        }};

        try {
            SSLContext sc = SSLContext.getInstance("TLS");

            sc.init(null, trustAllCerts, new java.security.SecureRandom());

            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (KeyManagementException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

}
