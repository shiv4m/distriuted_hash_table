package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SelectableChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Policy;
import java.util.ArrayList;
import java.util.Formatter;

import android.app.Activity;
import android.content.ContentProvider;
import android.content.ContentValues;

import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static java.lang.String.valueOf;
import java.lang.String;
import java.util.HashMap;


public class SimpleDhtProvider extends ContentProvider {
    private String myPort;
    HashMap<String, String> portToId= new HashMap<String, String>();
    private String predecessor = null;
    private String successor = null;
    private String node_id;
    String querySingleFound = null;
    String queryGlobalFound = null;


    //get file list - https://developer.android.com/reference/android/content/Context.html#fileList()
    //every thing else from previous PAs

    public void deleteLocalEverything(){
        Context context = getContext();
        String[] fileList = context.fileList();
        if(fileList.length > 0){
            for (int i=0; i<fileList.length; i++){
                File file = new File(context.getFilesDir(), fileList[i]);
                if(file.exists()){
                    file.delete();
                }
            }
        }
    }

    public void deleteSingleQuery(String key){
        Context context = getContext();
        File file = new File(context.getFilesDir(), key);
        if(file.exists()){
            file.delete();
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        //selection
        if(selection.equals("*")){
            if(predecessor == null && successor == null){
                deleteLocalEverything();
            }else{
                //delete queries from everywhere
                deleteLocalEverything();
                new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "deletestar-"+successor+"-"+myPort);
            }
        }else if(selection.equals("@")){
            deleteLocalEverything();
        }else{
            if(predecessor == null && successor == null){
                //delete local single query
                deleteSingleQuery(selection);
            }else{
                //find and delete query
                if(selfData(selection)){
                    deleteSingleQuery(selection);
                }else{
                    new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "deletesingle-"+successor+"-"+selection);
                }
            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.e("Insert : ", values.toString());
        String filename = values.get("key").toString();
        String filecontent = values.get("value").toString();
        checkAndPass(filename, filecontent);
        return null;
    }

    @Override
    public boolean onCreate() {
        portToId.put("11108","5554");
        portToId.put("11112","5556");
        portToId.put("11116","5558");
        portToId.put("11120","5560");
        portToId.put("11124","5562");

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portToStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = valueOf(Integer.parseInt(portToStr) * 2);
        Log.e("My port - ", myPort);

        try {
            node_id = genHash(portToId.get(myPort));
        }catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }

        try{
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch (IOException e){
            e.printStackTrace();
            Log.e("Error : ","Socket cannot be created");
        }


        if(!myPort.equals("11108")){
            new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "join-11108-"+myPort);
        }

        return false;
    }

    public String queryGlobal(String allData){
        Context context = getContext();
        String[] fileListO = context.fileList();
        for(int i=0; i <fileListO.length; i++) {
            Log.e("filename", fileListO[i]);
            File file = new File(context.getFilesDir(), fileListO[i]);
            StringBuilder txt = new StringBuilder();
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line;
                while ((line = br.readLine()) != null) {
                    txt.append(line);
                }
                if(allData.equals("")){
                    allData = allData + fileListO[i] + "-" + txt.toString();
                }else {
                    allData = allData + "-" + fileListO[i] + "-" + txt.toString();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return allData;
    }

    public Cursor parseGlobal(String gData){
        String[] n = gData.split("-");
        MatrixCursor mat = new MatrixCursor(new String[] {"key", "value"});
        for(int i=0; i < n.length; i++){
            mat.addRow(new Object[] {n[i], n[i+1]});
            i += 1;
        }
        return mat;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        querySingleFound = null;
        queryGlobalFound = null;
        Log.e("selection", selection);
        if(selection.equals("*")){
            if(predecessor == null && successor == null){
                Cursor mat = queryLocalDump();
                return mat;
            }
            else{
                String allData = "";
                allData = queryGlobal(allData);
                new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "globalquery--"+successor+"--"+allData+"--"+myPort+"--"+"false");
                //client call to successor
                while(true){
                    if(queryGlobalFound == null){
                        Log.e("w", "waiting for global");
                    }
                    else{
                        break;
                    }
                }
                Log.e("Data from global", queryGlobalFound);
                Cursor mat33 = parseGlobal(queryGlobalFound);
                return mat33;
            }
        }else if(selection.equals("@")){
            Cursor mat1 = queryLocalDump();
            return mat1;
        }
        else{
            if(predecessor == null && successor == null){
                String value = querySingleArg(selection);
                MatrixCursor mat = new MatrixCursor(new String[]{"key", "value"});
                mat.addRow(new Object[] {selection, value});
                return mat;
            }
            else{
                //check if key hash is mine
                if(selfData(selection)){
                    // if yes local simple check
                    String val = querySingleArg(selection);
                    MatrixCursor mat = new MatrixCursor(new String[]{"key", "value"});
                    mat.addRow(new Object[] {selection, val});
                    return mat;
                } else{
                    //else forward client call to successor
                    new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "querysingle-"+successor+"-"+selection+"-"+myPort);
                    while(true){
                        if(querySingleFound==null){
                            Log.e("Waiting for data", "Waiting...");
                        }else{
                            break;
                        }
                    }
                    MatrixCursor mat = new MatrixCursor(new String[]{"key", "value"});
                    mat.addRow(new Object[] {selection, querySingleFound});
                    return mat;
                }

            }
        }
    }

    public Cursor queryLocalDump(){
        Context context = getContext();
        Log.e("file list", context.fileList().toString());
        String[] fileListO = context.fileList();
        MatrixCursor mat = new MatrixCursor(new String[]{"key", "value"});
        for(int i=0; i <fileListO.length; i++) {
            Log.e("filename", fileListO[i]);
            File file = new File(context.getFilesDir(), fileListO[i]);
            StringBuilder txt = new StringBuilder();
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line;
                while ((line = br.readLine()) != null) {
                    txt.append(line);
                }
                mat.addRow(new Object[] {fileListO[i], txt.toString()});
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return mat;
    }

    public String querySingleArg(String key){
        Context context = getContext();
        File file = new File(context.getFilesDir(), key);
        StringBuilder txt = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while((line = br.readLine()) != null){
                txt.append(line);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return txt.toString();
    }

    public void singleQueryPass(String key, String originAvd){
        if(selfData(key)){
            String value = querySingleArg(key);
            new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "singlequeryfound-"+originAvd+"-"+value);
        }
        else{
            new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "querysingle-"+successor+"-"+key+"-"+originAvd);
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private void setPredecessorSucessor(String nodeR){
        try{
            ///nodeR 11108
            ///new_node 5554
            String new_node = portToId.get(nodeR);
            if(predecessor == null && successor == null){
                predecessor = successor = nodeR;
                new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "setps-"+nodeR+"-"+myPort+"-"+myPort);
                Log.e("s", "predcessor 1- "+predecessor);
                Log.e("s", "successor 2- "+successor);
            }
            else if(genHash(new_node).compareTo(node_id) <= 0 && genHash(new_node).compareTo(genHash(portToId.get(predecessor))) > 0){
                Log.e("dusre", "dusre condition me hu");
                new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "sets-"+predecessor+"-"+nodeR);
                new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "setps-"+nodeR+"-"+predecessor+"-"+myPort);
                predecessor = nodeR;
                Log.e("s", "predcessor 3- "+predecessor);
            }
            else if(genHash(new_node).compareTo(node_id) > 0){
                Log.e("idhar", "node_id>0 me hu");
                if(node_id.compareTo(genHash(portToId.get(successor))) > 0){
                    Log.e("idahr", "succe > 0 ,e huu");
                    new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "setps-"+nodeR+"-"+myPort+"-"+successor);
                    new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "setp-"+successor+"-"+nodeR);
                    successor = nodeR;
                    Log.e("s", "successor 4- "+successor);
                }else if(node_id.compareTo(genHash(portToId.get(successor))) <= 0){
                    Log.e("idhar", "succ < 0 me hu");
                    new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "join-" + successor + "-" + nodeR);
                }
            }

            else if(genHash(new_node).compareTo(node_id) <= 0){
                if(genHash(new_node).compareTo(genHash(portToId.get(predecessor))) > 0){
                    new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "sets-"+predecessor+"-"+nodeR);
                    new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "setps-"+nodeR+"-"+predecessor+"-"+myPort);
                    predecessor = nodeR;
                    Log.e("s", "predcessor 5- "+predecessor);
                }
                else if(genHash(new_node).compareTo(genHash(portToId.get(predecessor))) <= 0){
                    if(node_id.compareTo(genHash(portToId.get(predecessor))) > 0){
                        new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "join-" + predecessor + "-" + nodeR);
                    }
                    else if(node_id.compareTo(genHash(portToId.get(predecessor))) <= 0){
                        //bich me 0 hai
                        new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "sets-"+predecessor+"-"+nodeR);
                        new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "setps-"+nodeR+"-"+predecessor+"-"+myPort);
                        predecessor = nodeR;
                        Log.e("s", "predcessor 6- "+predecessor);
                    }
                }
            }

        }catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        protected Void doInBackground(ServerSocket... sockets){
            ServerSocket serverSocket = sockets[0];
            Log.e("Socket", "socket created");
            while(true){
                try {
                    Socket socket = serverSocket.accept();
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter pF = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    String message = br.readLine();
                    if(message != null && message.startsWith("join")){
                        socket.close();
                        String[] joinRequest = message.split("-");
                        String nodeR = joinRequest[1];
                        setPredecessorSucessor(nodeR);
                    }else if(message != null && message.startsWith("ps")){
                        socket.close();
                        String ps[] = message.split("-");
                        predecessor = ps[1];
                        successor = ps[2];
                        Log.e("s", "predecssor 6- "+predecessor);
                        Log.e("s", "successor 7- "+successor);
                    }else if(message != null && message.startsWith("sets")){
                        socket.close();
                        String sets[] = message.split("-");
                        successor = sets[1];
                        Log.e("s", "successor 8- "+successor);
                    }else if(message != null && message.startsWith("setp")){
                        socket.close();
                        String setp[] = message.split("-");
                        predecessor = setp[1];
                        Log.e("s", "predecssor 9- "+predecessor);
                    }else if(message != null && message.startsWith("insertdata")){
                        socket.close();
                        String data[] = message.split("-");
                        checkAndPass(data[1], data[2]);
                    }else if(message != null && message.startsWith("querysingle")){
                        socket.close();
                        String[] queryData = message.split("-");
                        singleQueryPass(queryData[1], queryData[2]);
                    }else if(message != null && message.startsWith("singlequeryfound")){
                        socket.close();
                        String[] querysingleFound = message.split("-");
                        querySingleFound = querysingleFound[1];
                    }else if(message != null && message.startsWith("globalquery")){
                        socket.close();
                        String[] globalquery = message.split("--");
                        //globalquery[1] = alldata, globalquery[2] = originavd, globalquery3 = final?
                        if(globalquery[3].equals("true")){
                            queryGlobalFound = globalquery[1];
                        }else{
                            String d = queryGlobal(globalquery[1]);
                            if(successor.equals(globalquery[2])){
                                String finall = "true";
                                callNextAvd(successor, d, globalquery[2], finall);
                            }else{
                                String finall = "false";
                                callNextAvd(successor, d, globalquery[2], finall);
                            }

                        }
                    }else if(message != null && message.startsWith("deletestar")){
                        socket.close();
                        String[] del = message.split("-");
                        deleteLocalEverything();
                        if(!successor.equals(del[1])){
                            nextDelAvd(successor, del[1]);
                        }
                    }else if(message != null && message.startsWith("deletesingle")){
                        socket.close();
                        String[] dell = message.split("-");
                        passAndDelete(dell[1]);
                    }
                }catch (IOException e){
                    e.printStackTrace();
                    Log.e("Error", "server ioexception");
                }
            }
        }
        protected void onProgressUpdate(){

        }
    }

    public void passAndDelete(String key){
        if(selfData(key)){
            deleteSingleQuery(key);
        }else{
            new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "deletesingle-"+successor+"-"+key);
        }
    }

    public void nextDelAvd(String successor, String originAvd){
        new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "deletestar-"+successor+"-"+originAvd);
    }

    public void callNextAvd(String successor, String allData1, String originAvd, String finall){
        new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "globalquery--"+successor+"--"+allData1+"--"+originAvd+"--"+finall);
    }

    private class ClienTask extends AsyncTask<String, Void, Void>{
        protected Void doInBackground(String... msgs){
            try {
                String[] request;
                if(msgs[0].startsWith("globalquery")){
                    request = msgs[0].split("--");
                }
                else {
                    request = msgs[0].split("-");
                }
                Log.e("recieved", request[0]);
                if(request[0].equals("join")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request[1]));
                    PrintWriter pf = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    //BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    pf.println("join-"+request[2]);
                    pf.flush();
                }else if(request[0].equals("setps")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request[1]));
                    PrintWriter pf = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    pf.println("ps-"+request[2]+"-"+request[3]);
                    pf.flush();
                }else if(request[0].equals("sets")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request[1]));
                    PrintWriter pf = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    pf.println("sets-"+request[2]);
                    pf.flush();
                }else if(request[0].equals("setp")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request[1]));
                    PrintWriter pf = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    pf.println("setp-"+request[2]);
                    pf.flush();
                }else if(request[0].equals("insertdata")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request[1]));
                    PrintWriter pf = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    pf.println("insertdata-"+request[2]+"-"+request[3]);
                    pf.flush();
                }else if(request[0].equals("querysingle")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request[1]));
                    PrintWriter pf = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    pf.println("querysingle-"+request[2]+"-"+request[3]);
                    pf.flush();
                }else if(request[0].equals("singlequeryfound")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request[1]));
                    PrintWriter pf = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    pf.println("singlequeryfound-"+request[2]);
                    pf.flush();
                }else if(request[0].equals("globalquery")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request[1]));
                    PrintWriter pf = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    pf.println("globalquery--"+request[2]+"--"+request[3]+"--"+request[4]);
                    pf.flush();
                }else if(request[0].equals("deletestar")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request[1]));
                    PrintWriter pf = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    pf.println("deletestar-"+request[2]);
                    pf.flush();
                }else if(request[0].equals("deletesingle")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(request[1]));
                    PrintWriter pf = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())));
                    pf.println("deletesingle-"+request[2]);
                    pf.flush();
                }
            }catch (IOException io){
                io.printStackTrace();
                Log.e("IOException","error sending node join request t o 5554");
                Log.e("e", "i'm the only node");
            }


            return null;
        }
    }

    public void insertData(String key, String value){
        Log.e("Inserting", key);
        Context con = getContext();
        FileOutputStream outputStream;
        try{
            File file = new File(con.getFilesDir(), key);
            if(file.exists())
                file.delete();
            outputStream = new FileOutputStream(new File(con.getFilesDir(), key));
            outputStream.write(value.getBytes());
            outputStream.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public boolean selfData(String key){
        try {
            if(predecessor == null && successor == null){
                //Log.e("pred succ null me hai","idhar data aaya");
                return true;
            }
            else if (genHash(key).compareTo(node_id) <= 0 && genHash(key).compareTo(genHash(portToId.get(predecessor))) > 0) {
                return true;
            }
            else if(node_id.compareTo(genHash(portToId.get(predecessor))) <= 0){
                if(genHash(key).compareTo(genHash(portToId.get(predecessor))) > 0 || genHash(key).compareTo(node_id) <= 0){
                    return true;
                }
            }else{
                return false;
            }
//            else if(genHash(key).compareTo(node_id) <= 0 && genHash(key).compareTo(genHash(portToId.get(predecessor))) < 0){
//                if(node_id.compareTo(genHash(portToId.get(predecessor))) < 0){
//                    return true;
//                }
//                else{return false;}
//            }
//            else if(genHash(key).compareTo(node_id) > 0 && genHash(key).compareTo(genHash(portToId.get(predecessor))) > 0){
//                return true;
//            }
//            else if(genHash(key).compareTo(node_id) > 0){
//                return false;
//            }
//            else if(genHash(key).compareTo(node_id) <= 0 && genHash(key).compareTo(genHash(portToId.get(successor))) <= 0){
//                return false;
//            }
        }catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        return false;
    }

    public void checkAndPass(String filename, String filecontent){
        //Log.e("rece in check and pass", filename+"-"+filecontent);
        //Log.e("selfdata", String.valueOf(selfData(filename)));
        if(selfData(filename)){
            //Log.e("selfdata", "ghus gaya");
            insertData(filename, filecontent);
        }
        else{
            //Log.e("clinetask me ghus gaya", ".");
            new ClienTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "insertdata-"+successor+"-"+filename+"-"+filecontent);
        }
    }

}
