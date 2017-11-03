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

import java.io.IOException;
import java.net.URLDecoder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 *
 * @author robegs
 */
public class MapperCollector extends Mapper<Object, Text, Text, Text> {

    public static final Log LOG = LogFactory.getLog(MapperCollector.class);
    public static final Pattern patTAB = Pattern.compile("\t");

    public static final String UA = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0";
    public static final String CFCOOKIE_NAME = "__cfduid";
    public static final String CFCOOKIE_VALUE = "dd57c1132d9ef915b2595fe7df3639c991470659991";

    public static final String[] PROXIES = {"proxy1", "proxy2"};
    
    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

//        int relevanceThreshold = context.getConfiguration().getInt("conf.relevance.threshold", 5);
//        try {
//            // String filename = ((FileSplit) context.getInputSplit()).getPath().toString();
//            System.out.println("relevanceThreshold: " + relevanceThreshold);
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println(e.getLocalizedMessage());
//
//        }
        int numTries = 0;
        boolean toCapture = true;
        while (toCapture && numTries < 5) {
            numTries++;
            try {

//            System.out.println(value.toString());
                String toks[] = patTAB.split(value.toString());
                if (toks.length >= 2) {
                    String cat = toks[0];
                    String text = toks[1];
//                System.out.println(cat + " - " + text);
                    text = text.replaceAll("&", "%26").replaceAll(" ", "+").replaceAll("_", "+");

//                System.out.println("host: " + context.getConfiguration().get("conf.host", ""));
//                    System.out.println("configure proxy");
                    String proxy = PROXIES[new Random().nextInt(PROXIES.length)];

                    System.out.println("Proxy: " + proxy);
                    System.setProperty("http.proxyHost", proxy);
                    System.setProperty("http.proxyPort", "3128");
                    System.setProperty("https.proxyHost", proxy);
                    System.setProperty("https.proxyPort", "3128");
                    System.setProperty("socksProxyHost", proxy);
                    System.setProperty("socksProxyPort", "3128");
                    disableSSLCertificateChecking();
                    // https://www.google.de/search?q=real+madrid&source=lnms&tbm=isch
                    String url = "https://www.google.de/search?q=" + text + "&source=lnms&tbm=isch";
                System.out.println(url);
                    Connection.Response response = Jsoup.connect(url)
                            .ignoreContentType(true)
                            .userAgent(UA)
                            .cookie(CFCOOKIE_NAME, CFCOOKIE_VALUE)
                            .timeout(25000)
                            .followRedirects(true)
                            .execute();

                    Document doc = response.parse();

                    List<String> allMatches = new ArrayList<String>();
                    Matcher m = Pattern.compile("\"ou\":\"(.*?)\",")
                            .matcher(doc.outerHtml());
                    int numImage = 0;
                    while (m.find()) {
                        String newURL = m.group();
//                    System.out.println(newURL);
                        newURL = newURL.substring(6);
                        newURL = newURL.substring(0, newURL.length() - 2);
                        //newURL = URLDecoder.decode(newURL, "utf8");
                        //newURL = "http://" + newURL;
//                    System.out.println(newURL);
                        context.write(new Text(newURL), new Text(cat + "/" + numImage++));

                    }
                    toCapture = false;

                }

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error in: " + value);
                System.out.println(e.getLocalizedMessage());

            }
        }
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
