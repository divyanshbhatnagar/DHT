package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;
import static edu.buffalo.cse.cse486586.simpledht.OnTestClickListener.KEY_FIELD;
import static edu.buffalo.cse.cse486586.simpledht.OnTestClickListener.VALUE_FIELD;

public class SimpleDhtProvider extends ContentProvider {

    public static String successor;
    public static String predecessor;
    public static ArrayList<String> dhtArr = new ArrayList<String>();
    public static String myPortstr;
    public static String origin;
    public static String msgType;
    private static Boolean lockSingleQuery = true;
    private static Boolean lockAllQuery = true;
    private static HashMap<String, String> returnHaspMap = new HashMap<String, String>();
    private static String returnAnswer;
    public static final String port0 = "11108";
    public Map<String, String> portMap = mapCreate();

    private Map<String, String> mapCreate() {
        Map<String, String> mapFinal = new HashMap<String, String>();
        try {
            mapFinal.put(genHash("5554"), "11108");
            mapFinal.put(genHash("5556"), "11112");
            mapFinal.put(genHash("5558"), "11116");
            mapFinal.put(genHash("5560"), "11120");
            mapFinal.put(genHash("5562"), "11124");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return mapFinal;
    }

    public static String portId[] = {"5554", "5556", "5558", "5560", "5562"};


    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT[] = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        SharedPreferences sharedPref = getContext().getSharedPreferences("DBFILE", 0);
        SharedPreferences.Editor editor = sharedPref.edit();
        editor.clear();
        editor.commit();
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = values.get(KEY_FIELD).toString();
        String value = values.get(VALUE_FIELD).toString();

        Log.v("TRIO", predecessor + ":" + myPortstr + ":" + successor);

        boolean condition = checkPosition(key);

        if (condition) {
            SharedPreferences sharedPref = getContext().getSharedPreferences("DBFILE", 0);
            SharedPreferences.Editor editor = sharedPref.edit();
            editor.putString(key, value);
            editor.commit();
            Log.v("Inserted Locally:  ", "value: " + value + " key:  " + key + " in port " + portMap.get(myPortstr));
        } else {
            //destination = portMap.get(successor);
            //Log.v("Request forwarded to", destination);
            msgType = "insert";
            Log.v("Insert Forwarded:  ", "value: " + value + " key:  " + key + "to port " + portMap.get(successor));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successor, predecessor, msgType, origin, portMap.get(successor), key, value);
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        // Networking hack from PA1
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        try {
            myPortstr = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");

        }

        origin = myPortstr;
        successor = myPortstr;
        predecessor = myPortstr;

        if (portStr.equals("5554")) {
            dhtArr.add(myPortstr);
            Log.v(TAG, "inserting 5554 in array" + myPortstr);
        } else {
            msgType = "join";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successor, predecessor, msgType, origin, port0, "", "");
        }

        return false;
    }

    public String insertMessage(String succ, String pred, String type, String origin, String dest, String key, String value) {
        try {

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(dest));

            Message mg = new Message(succ, pred, type, origin, dest, key, value);
            ObjectOutputStream obj = new ObjectOutputStream(socket.getOutputStream());
            obj.writeObject(mg);
            ObjectInputStream objRead = new ObjectInputStream(socket.getInputStream());
            Message message = (Message) objRead.readObject();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return "Successfully forwarded";

    }

    private String queryMessage(String succ, String pred, String type, String SrcOrigin, String dest, String key, String value) {
        try {

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(dest));

            Message mg = new Message(succ, pred, type, SrcOrigin, dest, key, value);
            ObjectOutputStream obj = new ObjectOutputStream(socket.getOutputStream());
            obj.writeObject(mg);
            ObjectInputStream objRead = new ObjectInputStream(socket.getInputStream());
            Message message = (Message) objRead.readObject();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return "Successfully forwarded";
    }

    private String queryAllMessage(String succ, String pred, String type, String SrcOrigin, String dest, String key, String value, HashMap<String, String> hMap) {
        try {

            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(dest));

            Message mg = new Message(succ, pred, type, SrcOrigin, dest, key, value, hMap);
            ObjectOutputStream obj = new ObjectOutputStream(socket.getOutputStream());
            obj.writeObject(mg);
            ObjectInputStream objRead = new ObjectInputStream(socket.getInputStream());
            Message message = (Message) objRead.readObject();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return "Successfully forwarded";
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            String authority = "edu.buffalo.cse.cse486586.simpledht.provider";
            String scheme = "content";

            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            Uri providerUri = uriBuilder.build();
            try {
                ServerSocket serverSocket = sockets[0];


            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
                //int keyToPut = 0;
                while (true) {

                    Socket socket = serverSocket.accept();
                    ObjectInputStream objRead = new ObjectInputStream(socket.getInputStream());
                    try {
                        Message msg = (Message) objRead.readObject();
                        switch (msg.mType) {
                            case "join":
                                String succToSend, preToSend;
                                dhtArr.add(msg.origin);
                                Log.v("Checking arr: ", dhtArr.size() + "");
                                Collections.sort(dhtArr);
                                int size = dhtArr.size();
                                int index = dhtArr.indexOf(msg.origin);
                                Log.v("Checking index: ", index + "");
                                if (index == 0) {
                                    Log.v("Checking index if 1: ", index + "");
                                    succToSend = dhtArr.get(index + 1);
                                    preToSend = dhtArr.get(size - 1);
                                    Log.v("Info1: ", "Succ : " + portMap.get(succToSend) + "Pred:  " + portMap.get(preToSend));
                                } else if (index == (size - 1)) {
                                    Log.v("Checking index if 2: ", index + "");
                                    succToSend = dhtArr.get(0);
                                    preToSend = dhtArr.get(size - 2);
                                    Log.v("Info2: ", "Succ : " + portMap.get(succToSend) + "Pred:  " + portMap.get(preToSend));
                                } else {
                                    Log.v("Checking index if 3: ", index + "");
                                    succToSend = dhtArr.get(index + 1);
                                    preToSend = dhtArr.get(index - 1);
                                    Log.v("Info3: ", "Succ : " + portMap.get(succToSend) + "Pred:  " + portMap.get(preToSend));
                                }
                                Log.v("Server", "join req from " + portMap.get(msg.origin) + ":: Sent Successor ::" + portMap.get(succToSend) + ":: Predecessor ::" + portMap.get(preToSend));

                                Message message = new Message(succToSend, preToSend, msg.origin);
                                ObjectOutputStream obj = new ObjectOutputStream(socket.getOutputStream());
                                obj.writeObject(message);

                                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,successor,predecessor,msgType,msg.origin, destination,msg);
                                Log.v("Server", "join req from " + portMap.get(msg.origin) + ":: Sent Predecessor Update to Successor::" + portMap.get(succToSend));
                                MessageAction("", msg.origin, "predUpdate", msg.origin, portMap.get(succToSend), "", "");

                                Log.v("Server", "join req from " + portMap.get(msg.origin) + ":: Sent Successor Update to Predecessor::" + portMap.get(preToSend));
                                MessageAction(msg.origin, "", "succUpdate", msg.origin, portMap.get(preToSend), "", "");
                                break;
                            case "predUpdate":


                                predecessor = msg.pred;
                                Log.v("Predecessor update req", portMap.get(predecessor));
                                message = new Message(successor, predecessor, msg.origin);
                                obj = new ObjectOutputStream(socket.getOutputStream());
                                obj.writeObject(message);
                                break;
                            case "succUpdate":
                                successor = msg.succ;
                                Log.v("Successor update req", portMap.get(successor));
                                message = new Message(successor, predecessor, msg.origin);
                                obj = new ObjectOutputStream(socket.getOutputStream());
                                obj.writeObject(message);
                                break;
                            case "insert":
                                Log.v("IRequest from:  ", portMap.get(msg.origin) + " key:  " + msg.key + " value " + msg.value);
                                boolean condition = checkPosition(msg.key);
                                if (condition == true) {

                                    Log.v("Inserted Remotely:  ", "value: " + msg.value + " key:  " + msg.key + " in port " + portMap.get(myPortstr) + " fromn port " + portMap.get(msg.origin));
                                    SharedPreferences sharedPref = getContext().getSharedPreferences("DBFILE", 0);
                                    SharedPreferences.Editor editor = sharedPref.edit();
                                    editor.putString(msg.key, msg.value);
                                    editor.commit();

                                } else {
                                    msgType = "insert";
                                    Log.v("Forwarded Remotely:  ", "value: " + msg.value + " key:  " + msg.key + " to port " + portMap.get(successor) + " fromn port " + portMap.get(msg.origin));
                                    insertMessage(successor, predecessor, msgType, origin, portMap.get(successor), msg.key, msg.value);
                                }

                                message = new Message(successor, predecessor, msg.origin);
                                obj = new ObjectOutputStream(socket.getOutputStream());
                                obj.writeObject(message);
                                break;
                            case "singlequery":
                                if(checkPosition(msg.key))
                                {
                                    Log.v("Searched Remotely:  ", "value: " + msg.value + " key:  " + msg.key + " to port " + portMap.get(msg.origin) + " from port " + portMap.get(myPortstr) + " Origin" + portMap.get(msg.origin));
                                    SharedPreferences sharedPref = getContext().getSharedPreferences("DBFILE", 0);
                                    String ans = sharedPref.getString(msg.key, null);
                                    queryMessage(successor, predecessor, "singleresult", msg.origin, portMap.get(msg.origin), msg.key, ans);
                                } else {
                                    Log.v("Search Sent Remote:  ", "value: " + msg.value + " key:  " + msg.key + " to port " + portMap.get(successor) + " from port " + portMap.get(myPortstr) + " Origin" + portMap.get(msg.origin));
                                    queryMessage(successor, predecessor, "singlequery", msg.origin, portMap.get(successor), msg.key, msg.value);
                                }
                                message = new Message(successor, predecessor, msg.origin);
                                obj = new ObjectOutputStream(socket.getOutputStream());
                                obj.writeObject(message);
                                break;
                            case "singleresult":
                                returnAnswer = msg.value;
                                lockSingleQuery = false;
                                message = new Message(successor, predecessor, msg.origin);
                                obj = new ObjectOutputStream(socket.getOutputStream());
                                obj.writeObject(message);
                                break;
                            case "allquery":
                                if (myPortstr.equals(msg.origin))
                                {
                                    lockAllQuery = false;
                                }
                                else {
                                    SharedPreferences sharedPref = getContext().getSharedPreferences("DBFILE", 0);
                                    HashMap<String, String> tempMap = new HashMap<String, String>();
                                    for (String key : sharedPref.getAll().keySet()) {
                                        tempMap.put(key, sharedPref.getString(key, null));
                                    }
                                    Log.v("Searched ALL Rem:  ", " key:  " + msg.key + " to port " + portMap.get(successor) + " from port " + portMap.get(portMap.get(myPortstr)));
                                    queryAllMessage(successor, predecessor, "allresult", msg.origin, portMap.get(msg.origin), msg.key, msg.value, tempMap);
                                    queryAllMessage(successor, predecessor, "allquery", msg.origin, portMap.get(successor), msg.key, msg.value, null);
                                }
                                message = new Message(successor, predecessor, msg.origin);
                                obj = new ObjectOutputStream(socket.getOutputStream());
                                obj.writeObject(message);
                                break;
                            case "allresult":
                                returnHaspMap.putAll(msg.allQResult);
                                message = new Message(successor, predecessor, msg.origin);
                                obj = new ObjectOutputStream(socket.getOutputStream());
                                obj.writeObject(message);
                                break;
                            default:
                                Log.e(TAG, "Unknown message type: " + msg.mType);


                        }

                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (IndexOutOfBoundsException e) {
                        Log.e(TAG, "Index not found  ", e);
                    }
                    /*catch (NoSuchAlgorithmException e){
                        Log.e(TAG,"No algo  ",e);
                    }*/


                }
            } catch (IOException e) {
                Log.e(TAG, "Error in server socket");
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {


            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                String succ = msgs[0];
                String pred = msgs[1];
                String msgTypeMo = msgs[2];
                String sendOrigin = msgs[3];
                String dest = msgs[4];
                String sendkey = msgs[5];
                String sendValue = msgs[6];

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(dest));

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */

                Message mg = new Message(succ, pred, msgTypeMo, sendOrigin, dest, sendkey, sendValue);
                ObjectOutputStream obj = new ObjectOutputStream(socket.getOutputStream());
                obj.writeObject(mg);

                ObjectInputStream objRead = new ObjectInputStream(socket.getInputStream());
                Message message = (Message) objRead.readObject();

                if ((msgTypeMo.equals("join"))) {
                    successor = message.succ;
                    predecessor = message.pred;
                    Log.e(TAG, msgTypeMo + " Client info: successor " + portMap.get(successor) + "Predecessor " + portMap.get(predecessor));
                    //socket.close();
                }
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "" + e);
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException" + e);
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException" + e);
            }

            return null;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        String query = selection;
        //Map<String, String> temp = new HashMap<String,String>();
        String[] params = {"key", "value"};
        MatrixCursor mtxCur = new MatrixCursor(params);
        if (query.equals("@")) {
            SharedPreferences sharedPref = getContext().getSharedPreferences("DBFILE", 0);
            Map<String, ?> temp = sharedPref.getAll();
            for (String key : temp.keySet()) {
                Log.v("Stored keys @  ", key);
                String ans = temp.get(key).toString();
                Log.v("Stored values @   ", ans);
                mtxCur.addRow(new String[]{key, ans});
            }
            // return mtxCur;
        } else if (query.equals("*")) {
            SharedPreferences sharedPref = getContext().getSharedPreferences("DBFILE", 0);
            Map<String, ?> temp = sharedPref.getAll();

            for (String key : temp.keySet()) {
                Log.v("Stored keys *  ", key);
                String ans = temp.get(key).toString();
                Log.v("Stored values *  ", ans);
                mtxCur.addRow(new String[]{key, ans});
            }
            Log.v("Search ALL SentL:  ", " key:  " + selection + " to port " + portMap.get(successor) + " from port " + portMap.get(portMap.get(myPortstr)));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successor, predecessor, "allquery", origin, portMap.get(successor), selection, "");
            while (lockAllQuery){
                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            lockAllQuery = true;
            for (String hkey : returnHaspMap.keySet()){
                mtxCur.addRow(new String[]{hkey, returnHaspMap.get(hkey)});
            }
        } else {
            if (checkPosition(selection)) {
                SharedPreferences sharedPref = getContext().getSharedPreferences("DBFILE", 0);
                String ans = sharedPref.getString(query, null);
                Log.v("Returning", "key " + query + "Value " + ans);
                mtxCur.addRow(new String[]{selection, ans});
            } else
            {
                Log.v("Search Sent Local:  ", " key:  " + selection + " to port " + portMap.get(successor) + " from port " + portMap.get(myPortstr));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, successor, predecessor, "singlequery", origin, portMap.get(successor), selection, "");
                while (lockSingleQuery){
                    try {
                        TimeUnit.MILLISECONDS.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                mtxCur.addRow(new String[]{selection, returnAnswer});
                lockSingleQuery = true;
            }
        }
        return mtxCur;
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

    public void MessageAction(String succ, String pred, String type, String origin, String dest, String key, String value) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, succ, pred, type, origin, dest, key, value);
    }

    public boolean checkPosition(String keyed) {
        //Log.v("The entered key ", keyed);
        String hashOfKey = null;
        try {
            hashOfKey = genHash(keyed);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception E) {
            E.printStackTrace();
        }

        Log.v("Condition :   ", "Port:   " + myPortstr + "Pred:     " + predecessor + "HashedKey:    " + hashOfKey);
        if ((hashOfKey.compareTo(myPortstr) <= 0 && hashOfKey.compareTo(predecessor) > 0))  {
            Log.v("Condition Check", "Condition for interior Nodes statisfied");
            return true;
        } else if (((predecessor.compareTo(myPortstr) >= 0 && hashOfKey.compareTo(predecessor) > 0) || (predecessor.compareTo(myPortstr) >= 0 && hashOfKey.compareTo(myPortstr) <= 0)))
        {
            Log.v("Condition Check", "Condition for end nodes statisfied");
            return true;
        } else {
            Log.v("FailedC", hashOfKey.compareTo(myPortstr) + " : " + hashOfKey.compareTo(predecessor) + " : " + myPortstr.compareTo(predecessor));
           /* destination = portMap.get(successor);
            Log.v("Request forwarded to", destination);
            msgType = "insert";
            MessageAction(successor,predecessor,msgType,origin,destination,key,value);*/
            return false;
        }
    }

   /* public Cursor queryCheck(){

    }
*/
}

