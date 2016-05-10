/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mgmload;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.URL;
import java.util.Date;

/**
 *
 * @author jwaldorf
 */
public class Load implements Runnable {

    public String urlPrefix;
    public String token;
    public int id;
    public int threads;
    public String inputFileString;
    public String proxyhost;
    public String port;
    public String username;
    public String password;
    public boolean doProxy;

    public String post(String resource, String token, String data, Proxy proxy) {
        String res = "";
        try {
            URL url = new URL(resource);
            HttpURLConnection conn;
            if (proxy != null) {
                conn = (HttpURLConnection) url.openConnection(proxy);
            } else {
                conn = (HttpURLConnection) url.openConnection();
            }
            conn.setConnectTimeout(100000);
            conn.setReadTimeout(100000);
            conn.setDoOutput(true);
            conn.setRequestProperty("Authorization", "SSWS " + token);
            conn.setRequestProperty("Content-Type", "application/json");

            conn.setRequestMethod("POST");
            DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
            wr.write(data.getBytes("UTF-8"));
            wr.flush();
            wr.close();
            String line;
            if (conn.getResponseCode() == 200 || conn.getResponseCode() == 201) {
                BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                while ((line = rd.readLine()) != null) {
                    res += line;
                }
                rd.close();
            } else if (conn.getResponseCode() == 204) {
            } else {
                BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
                while ((line = rd.readLine()) != null) {
                    res += line;
                }
                rd.close();
                System.out.println(new Date() + " POST " + url.toString() + " OF " + data + " RETURNS " + conn.getResponseCode() + ":" + conn.getResponseMessage());
                System.out.println(new Date() + " ERRORSTREAM = " + res);
                res = "";
            }
            int rateLimitLimit = 0;
            int rateLimitRemaining = 0;
            int rateLimitReset = 0;
            String rll = conn.getHeaderField("X-Rate-Limit-Limit");
            String rlr = conn.getHeaderField("X-Rate-Limit-Remaining");
            String rlreset = conn.getHeaderField("X-Rate-Limit-Reset");
            if (rll != null && rll.length() > 0) {
                rateLimitLimit = Integer.parseInt(rll);
            }
            if (rlr != null && rlr.length() > 0) {
                rateLimitRemaining = Integer.parseInt(rlr);
            }
            if (rlreset != null && rlreset.length() > 0) {
                rateLimitReset = Integer.parseInt(rlreset);
            }
            System.out.println("rateLimitRemaining = " + rateLimitRemaining
                    + " rateLimitLimit = " + rateLimitLimit
                    + " rateLimitReset = " + rateLimitReset
                    + " current Time = " + System.currentTimeMillis() / 1000);
            if (rateLimitRemaining < threads + threads) {
                long secs = rateLimitReset + 10 - (System.currentTimeMillis() / 1000);
                System.out.println("Sleeping for " + secs);
                try {
                    Thread.sleep(secs * 1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.out.println(new Date() + " POST " + resource + " OF " + data + " LOCALIZEDMESSAGE " + e.getLocalizedMessage());
            res = e.getLocalizedMessage();
            e.printStackTrace();
            res = "";
        }
        return res;
    }

    public void run() {
        try {
            String inputFileString = this.inputFileString + id + ".csv";
            String failFileString = this.inputFileString + id + "-fail.csv";
            String countFile = this.inputFileString + id + "-index.txt";
            int start = 0;

            BufferedReader brr = null;
            String bline = "";
            try {
                brr = new BufferedReader(new FileReader(countFile));
                while ((bline = brr.readLine()) != null) {
                    start = Integer.parseInt(bline);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (brr != null) {
                    try {
                        brr.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            Proxy proxy = null;
            if (doProxy) {
                int portInt = Integer.parseInt(port);
                proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyhost, portInt));
                Authenticator authenticator = new Authenticator() {
                    public PasswordAuthentication getPasswordAuthentication() {
                        return (new PasswordAuthentication(username, password.toCharArray()));
                    }
                };
                Authenticator.setDefault(authenticator);
            }
            int count = 0;
            BufferedReader br = new BufferedReader(new FileReader(inputFileString));
            String line = br.readLine();
            while (line != null) {
                if (count >= start) {
                    int v = 0;
                    String[] values = new String[8];
                    values[0] = "";
                    values[1] = "";
                    values[2] = "";
                    values[3] = "";
                    values[4] = "";
                    values[5] = "";
                    values[6] = "";
                    values[7] = "";
                    for (int i = 0; i < line.length(); i++) {
                        char c = line.charAt(i);
                        if (c == ',') {
                            v++;
                        } else if (c == '/') {
                            i++;
                            values[v] += line.charAt(i);
                        } else {
                            values[v] += c;
                        }
                    }
                    String ret = post(urlPrefix + "/users?activate=true", token,
                            "{\"profile\":{"
                            + "\"login\":\"" + values[0] + "\","
                            + "\"firstName\":\"" + ((values[1].trim().length() > 0) ? values[1] : "-") + "\","
                            + "\"lastName\":\"" + ((values[2].trim().length() > 0) ? values[2] : "-") + "\","
                            + "\"email\":\"" + values[0] + "\","
                            + "\"lastPasswordUpdated\":\"" + values[3] + "\","
                            + "\"oldPasswordHash\":\"" + values[4] + "\","
                            + "\"mlifeNumber\":\"" + values[5] + "\","
                            + "\"secretQuestionId\":\"" + values[6] + "\","
                            + "\"secretQuestionAnswerHash\":\"" + values[7] + "\""
                            + "},"
                            + "  \"credentials\": {"
                            + "    \"password\" : { \"value\": \"tlpWENT2m\" }"
                            + "  }}", proxy);
                    if (ret.length() == 0) {
                        PrintWriter pw = new PrintWriter(new FileOutputStream(new File(failFileString), true));
                        pw.println(line);
                        pw.close();
                    } else {
                        JSONObject o = new JSONObject(ret);
                        if (o.has("errorCode")) {
                            PrintWriter pw = new PrintWriter(new FileOutputStream(new File(failFileString), true));
                            pw.println(line);
                            pw.close();
                        }
                    }
                }
                count++;
                if (count > start) {
                    try {
                        File file = new File(countFile);
                        if (!file.exists()) {
                            file.createNewFile();
                        }
                        FileWriter fw = new FileWriter(file.getAbsoluteFile());
                        BufferedWriter bw = new BufferedWriter(fw);
                        bw.write("" + count);
                        bw.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("" + count);
                line = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        long st = System.currentTimeMillis();
        String GurlPrefix = args[0];
        String Gtoken = args[1];
        String GinputFileString = args[2];
        int Gthreads = Integer.parseInt(args[3]);
        boolean GdoProxy = false;
        String Gproxyhost = "";
        String Gport = "";
        String Gusername = "";
        String Gpassword = "";
        if (args.length > 4) {
            GdoProxy = true;
            Gproxyhost = args[4];
            Gport = args[5];
            Gusername = args[6];
            Gpassword = args[7];
        }

        Load[] thd = new Load[Gthreads];
        Thread[] t = new Thread[Gthreads];
        for (int i = 0; i < Gthreads; i++) {
            thd[i] = new Load();
            thd[i].id = i;
            thd[i].token = Gtoken;
            thd[i].urlPrefix = GurlPrefix;
            thd[i].inputFileString = GinputFileString;
            thd[i].doProxy = GdoProxy;
            thd[i].proxyhost = Gproxyhost;
            thd[i].port = Gport;
            thd[i].username = Gusername;
            thd[i].password = Gpassword;
            thd[i].threads = Gthreads;
            t[i] = new Thread(thd[i]);
            t[i].start();
        }

        for (int i = 0; i < Gthreads; i++) {
            try {
                t[i].join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long et = System.currentTimeMillis();
        System.out.println("Total Seconds = " + (et - st) / 1000);
    }
}
