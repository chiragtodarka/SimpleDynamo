package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider 
{

	static SimpleDynamoDatabase simpleDynamoDatabase;
	
	static String portStr;
	static String nodeId;
	
	static String predecessor;
	static String predecessorNodeId;
	
	static String successor;
	static String successorNodeId;

	//static int versionNumber;
	static Message queryResponseMessage;
	static boolean queryResponseReceived;
	
	final static String REPLICATE_STRING = "replicate";
	final static String REQUESTOR_IS_COORDINATOR = "requestorIsCoordinator";
	final static String JUST_ADD = "justAdd";
	final static String PUT = "put";
	final static String PUT1 = "put1";
	final static String PUT_ACK = "putAck";
	final static String RECOVERY = "recovery";
	final static String RECOVERY_ACK = "recoveryAck";
	final static String GET = "get";
	final static String GET_ACK = "getAck";
	//static int[] versionArray;
	static HashMap<String, Integer> versionArray;
	
	public static Lock myLock = new ReentrantLock();
	
	@Override
	public boolean onCreate()
	{
		try 
    	{
			SimpleDynamoProvider.simpleDynamoDatabase = new SimpleDynamoDatabase(getContext());
			
			TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
			
			SimpleDynamoProvider.portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			SimpleDynamoProvider.nodeId = genHash(SimpleDynamoProvider.portStr);
			
			Integer portStrInteger = Integer.parseInt(SimpleDynamoProvider.portStr);
			
			switch(portStrInteger)
			{
				case 5554:
					SimpleDynamoProvider.predecessor = "5556";
					SimpleDynamoProvider.successor = "5558";
				break;
				
				case 5556:
					SimpleDynamoProvider.predecessor = "5558";
					SimpleDynamoProvider.successor = "5554";
				break;
				
				case 5558:
					SimpleDynamoProvider.predecessor = "5554";
					SimpleDynamoProvider.successor = "5556";
				break;
			
				default:
					SimpleDynamoProvider.predecessor = "";
					SimpleDynamoProvider.successor = "";
				break;
			}
			
			SimpleDynamoProvider.predecessorNodeId = genHash(SimpleDynamoProvider.predecessor);
			SimpleDynamoProvider.successorNodeId = genHash(SimpleDynamoProvider.successor);
			
			//versionArray = new int[OnPutClickListener.TEST_COUNT];
			versionArray = new HashMap<String, Integer>();
			
			queryResponseMessage = null;
			queryResponseReceived = false;
			
			new MyServer().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, (Void)null);
			
			Message message = new Message(SimpleDynamoProvider.portStr, SimpleDynamoProvider.successor, 
					SimpleDynamoProvider.RECOVERY, "", "", 0);
			new MyClient().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message);
			Log.v("Node:", SimpleDynamoProvider.portStr);
			Log.v("Successor:", SimpleDynamoProvider.successor);
			Log.v("Predessor:", SimpleDynamoProvider.predecessor);
			Log.v("Recovery msg send", "Recovery msg send");
		} 
    	catch (NoSuchAlgorithmException err) 
    	{
			Log.e("Error in genHash: ", err.getMessage());
		}
		
		return true;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) 
	{
		String key = (String) values.get("key");
		String value = (String) values.get("value");
		
//		if(key.startsWith("key"))
//		{
//			SQLiteDatabase sqlDB = SimpleDynamoProvider.simpleDynamoDatabase.getWritableDatabase();
//			ContentValues contentValues = new ContentValues();
//			contentValues.put("key", key);
//			contentValues.put("value", value);
//			sqlDB.insertWithOnConflict(SimpleDynamoDatabase.TABLE_NAME, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
//			return uri;
//		}
		
		String coordinator = findCoordinator(key);
				
		//Log.v("PutClicked: Coordinator:Key:Value:Version", coordinator+":"+key+":"+value+":"+SimpleDynamoProvider.versionArray[Integer.parseInt(key)]);
				
		if(coordinator.equalsIgnoreCase(SimpleDynamoProvider.portStr))
		{
			SimpleDynamoProvider.insertMessage(key, value, SimpleDynamoProvider.REQUESTOR_IS_COORDINATOR);
			//Log.v("Adding Key...", "I am coordinator");
		}
		else
		{
			Message message = new Message(SimpleDynamoProvider.portStr, coordinator, SimpleDynamoProvider.PUT,
					key, value, SimpleDynamoProvider.getVersionNumber(key));
			new MyClient().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message);
			//Log.v("Key Forwarded", "I am NOT coordinator");
		}
		
		return uri;
	}
	
	public static void insertMessage(String key, String value, String messageType)
	{
		if(messageType.equalsIgnoreCase(SimpleDynamoProvider.REQUESTOR_IS_COORDINATOR))
		{
			SQLiteDatabase sqlDB = SimpleDynamoProvider.simpleDynamoDatabase.getWritableDatabase();
			ContentValues contentValues = new ContentValues();
			contentValues.put("key", key);
			contentValues.put("value", value);
			
			myLock.lock();
			sqlDB.insertWithOnConflict(SimpleDynamoDatabase.TABLE_NAME, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
		
			//SimpleDynamoProvider.versionArray[Integer.parseInt(key)]++;
			Integer oldVer = SimpleDynamoProvider.versionArray.get(key);
			if(oldVer==null)
			{
				oldVer = 0;
			}
			oldVer++;
			SimpleDynamoProvider.versionArray.put(key, oldVer);
			
			Log.v("Key Added locally", key+":"+value);
			
			Message message1 = new Message(SimpleDynamoProvider.portStr, SimpleDynamoProvider.successor, 
					SimpleDynamoProvider.REPLICATE_STRING, key, value, SimpleDynamoProvider.versionArray.get(key));
			//Log.v("KeySend for replication to "+SimpleDynamoProvider.successor, key+":"+SimpleDynamoProvider.versionArray[Integer.parseInt(key)]);
			
			Message message2 = new Message(SimpleDynamoProvider.portStr, SimpleDynamoProvider.predecessor, 
					SimpleDynamoProvider.REPLICATE_STRING, key, value, SimpleDynamoProvider.versionArray.get(key));
			//Log.v("KeySend for replication to "+SimpleDynamoProvider.predecessor, key+":"+SimpleDynamoProvider.versionArray[Integer.parseInt(key)]);
			
			new MyClient().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message1, message2);
			myLock.unlock();
		}
		else if(messageType.equalsIgnoreCase(SimpleDynamoProvider.JUST_ADD))
		{
			SQLiteDatabase sqlDB = SimpleDynamoProvider.simpleDynamoDatabase.getWritableDatabase();
			ContentValues contentValues = new ContentValues();
			contentValues.put("key", key);
			contentValues.put("value", value);
			sqlDB.insertWithOnConflict(SimpleDynamoDatabase.TABLE_NAME, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) 
	{
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(SimpleDynamoDatabase.TABLE_NAME);
		SQLiteDatabase db = SimpleDynamoProvider.simpleDynamoDatabase.getReadableDatabase();
		Cursor cursor;
		if(selection.equalsIgnoreCase("LDUMP"))
		{
			cursor = queryBuilder.query(db, null, null, null, null, null, null);
			return cursor;
		}
//		else if(selection.startsWith("key"))
//		{
//			cursor = queryBuilder.query(db, null, SimpleDynamoDatabase.TABLE_NAME+"."+SimpleDynamoDatabase.COLUMN_KEY+"='"+selection+"'", null, null, null, null);
//			return cursor;
//		}
		else
		{
			String coordinator = findCoordinator(selection);
			//if(coordinator.equalsIgnoreCase(SimpleDynamoProvider.portStr))
			{
				Log.v("Query"+selection, "key fwd to "+ coordinator);
				cursor = queryBuilder.query(db, null, SimpleDynamoDatabase.TABLE_NAME+"."+SimpleDynamoDatabase.COLUMN_KEY+"='"+selection+"'", null, null, null, null);
				try 
				{
					Thread.sleep(1000);
				} 
				catch (InterruptedException e) 
				{
					Log.e("InterruptedException in query", e.getMessage());
				}
				return cursor;
			}
//			else
//			{
//				Log.v("Query"+selection, "key fwd to "+ coordinator);
//				Message message = new Message(SimpleDynamoProvider.portStr, coordinator, SimpleDynamoProvider.GET, 
//						selection, "", SimpleDynamoProvider.getVersionNumber(selection));
//				
//				queryResponseMessage = null;
//				queryResponseReceived = false;
//				new MyClient().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message);
//				try 
//				{
//					Thread.sleep(1000);
//				} 
//				catch (InterruptedException e) 
//				{
//					Log.e("InterruptedException in query", e.getMessage());
//				}
//				if(queryResponseMessage!=null && queryResponseReceived)
//				{
//					MatrixCursor newCursor= new MatrixCursor(new String[]{"key","value"});
//					newCursor.addRow(new String[]{queryResponseMessage.key, queryResponseMessage.value});
//					cursor = newCursor;
//					return cursor;
//				}
//				else
//				{
//					return null;
//				}
//			}
			
			//cursor = null;
		}
		//cursor.setNotificationUri(getContext().getContentResolver(), uri);
		//return cursor;
	}
	
	public static String getValue(String key)
	{
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(SimpleDynamoDatabase.TABLE_NAME);
		SQLiteDatabase db = SimpleDynamoProvider.simpleDynamoDatabase.getReadableDatabase();
		Cursor cursor = queryBuilder.query(db, null, SimpleDynamoDatabase.TABLE_NAME+"."+SimpleDynamoDatabase.COLUMN_KEY+"='"+key+"'", null, null, null, null);
		
		String value = "";
		if(cursor.getCount()==1)
		{
			//int valueIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_VALUE);
			//Log.v("valueIndex", valueIndex+"");
			int keyIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_KEY);
			int valueIndex = cursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_VALUE);
			if (keyIndex == -1 || valueIndex == -1) 
			{
				Log.e("SimpleDynamoProvider.getValue", "Wrong columns");
				cursor.close();
				//throw new Exception();
			}
			cursor.moveToFirst();
			value = cursor.getString(valueIndex);
			cursor.close();
			return value;
		}
		else
		{
			Log.e("More than one row", key);
			return null;
		}
	}
	
	public static Cursor getDataForRecoveryAck()
	{
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(SimpleDynamoDatabase.TABLE_NAME);
		SQLiteDatabase db = SimpleDynamoProvider.simpleDynamoDatabase.getReadableDatabase();
		Cursor cursor = queryBuilder.query(db, null, null, null, null, null, null);
		return cursor;
	}
	
	public String findCoordinator(String key)
	{
		try
		{
			String keyHash = genHash(key);
					
			boolean flag1 = (SimpleDynamoProvider.predecessorNodeId.compareTo(SimpleDynamoProvider.nodeId)>0) && (keyHash.compareTo(SimpleDynamoProvider.predecessorNodeId)>0);
			boolean flag2 = (keyHash.compareTo(SimpleDynamoProvider.nodeId) <= 0) && (keyHash.compareTo(SimpleDynamoProvider.predecessorNodeId)>0 );
			boolean flag3 = (keyHash.compareTo(SimpleDynamoProvider.nodeId) <= 0) && (SimpleDynamoProvider.predecessorNodeId.compareTo(SimpleDynamoProvider.nodeId)>0);
			boolean flag4 = ((SimpleDynamoProvider.portStr.equalsIgnoreCase(SimpleDynamoProvider.predecessor)) && (SimpleDynamoProvider.portStr.equalsIgnoreCase(SimpleDynamoProvider.successor)));
			if(flag1 || flag2 || flag3 || flag4)
			{
				return SimpleDynamoProvider.portStr;
			}
			else
			{
				String tempNodeID = SimpleDynamoProvider.successorNodeId;
				String tempPredessorId = SimpleDynamoProvider.nodeId;
				String tempSuccessprId = SimpleDynamoProvider.predecessorNodeId;
				
				boolean flag5 = (tempPredessorId.compareTo(tempNodeID)>0) && (keyHash.compareTo(tempPredessorId)>0);
				boolean flag6 = (keyHash.compareTo(tempNodeID) <= 0) && (keyHash.compareTo(tempPredessorId)>0 );
				boolean flag7 = (keyHash.compareTo(tempNodeID) <= 0) && (tempPredessorId.compareTo(tempNodeID)>0);
				boolean flag8 = ((tempNodeID.equalsIgnoreCase(tempPredessorId)) && (tempNodeID.equalsIgnoreCase(tempSuccessprId)));
				if(flag5 || flag6 || flag7 || flag8)
					return SimpleDynamoProvider.successor;
				
				tempNodeID = SimpleDynamoProvider.predecessorNodeId;
				tempPredessorId = SimpleDynamoProvider.successorNodeId;
				tempSuccessprId = SimpleDynamoProvider.nodeId;
				
				boolean flag9 = (tempPredessorId.compareTo(tempNodeID)>0) && (keyHash.compareTo(tempPredessorId)>0);
				boolean flag10 = (keyHash.compareTo(tempNodeID) <= 0) && (keyHash.compareTo(tempPredessorId)>0 );
				boolean flag11 = (keyHash.compareTo(tempNodeID) <= 0) && (tempPredessorId.compareTo(tempNodeID)>0);
				boolean flag12 = ((tempNodeID.equalsIgnoreCase(tempPredessorId)) && (tempNodeID.equalsIgnoreCase(tempSuccessprId)));
				if(flag9 || flag10 || flag11 || flag12)
					return SimpleDynamoProvider.predecessor;
			}
			return null;
			
		}
		catch(NoSuchAlgorithmException e)
		{
			Log.v("Error in findCoordinator", e.getMessage());
			return null;
		}
	}

    private String genHash(String input) throws NoSuchAlgorithmException 
    {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    
    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public static Integer getVersionNumber(String key)
	{
		Integer versionNumber = 0;
		if(SimpleDynamoProvider.versionArray.containsKey(key))
			versionNumber =  SimpleDynamoProvider.versionArray.get(key);
			
		return versionNumber;
	}

}

class MyServer extends AsyncTask<Void, Void, Void> 
{
	@Override
	protected Void doInBackground(Void... arg0) 
	{
		try
		{
			ServerSocket serverSocket = new ServerSocket(10000);
			Socket listener;
			while(true)
			{
				listener = serverSocket.accept();
				ObjectInputStream ois = new ObjectInputStream(listener.getInputStream());
				Message message = (Message)ois.readObject();
				
				if(message.messageType.equalsIgnoreCase(SimpleDynamoProvider.REPLICATE_STRING))
				{
					if(message.versionNumber>SimpleDynamoProvider.getVersionNumber(message.key))
					{
						Log.v("REPLICATE_STRING string received Adding", SimpleDynamoProvider.versionArray.get(message.key)+":"+message.key+":"+message.value+":"+message.versionNumber);
						//SimpleDynamoProvider.versionArray[Integer.parseInt(message.key)] = message.versionNumber;
						SimpleDynamoProvider.myLock.lock();
						SimpleDynamoProvider.versionArray.put(message.key, message.versionNumber);
						SimpleDynamoProvider.insertMessage(message.key, message.value, SimpleDynamoProvider.JUST_ADD);
						SimpleDynamoProvider.myLock.unlock();
					}
					else
					{
						Log.e("REPLICATE_STRING string received Ignored, MYversion:key:value:msgVersion", SimpleDynamoProvider.versionArray.get(message.key)+":"+message.key+":"+message.value+":"+message.versionNumber);
					}
				}
				else if(message.messageType.equalsIgnoreCase(SimpleDynamoProvider.RECOVERY_ACK))
				{
					//if(message.versionNumber>SimpleDynamoProvider.versionArray[0])
					{
						//Log.v("RECOVERY_ACK string received Adding", SimpleDynamoProvider.versionArray[Integer.parseInt(message.key)]+":"+message.key+":"+message.value+":"+message.versionNumber);
						//SimpleDynamoProvider.versionArray[Integer.parseInt(message.key)] = message.versionNumber;
						
						String[] combine = message.key.split("#");
						String[] keyArray = combine[0].split(":");
						String[] versionString = combine[1].split(":");
						String[] valueArray = message.value.split(":");
						if(keyArray.length==valueArray.length && valueArray.length==versionString.length)
						{
							for(int count=0 ; count<keyArray.length; count++)
							{
								SimpleDynamoProvider.myLock.lock();
								SimpleDynamoProvider.insertMessage(keyArray[count], valueArray[count], SimpleDynamoProvider.JUST_ADD);
								//SimpleDynamoProvider.versionArray[Integer.parseInt(keyArray[count])] = Integer.parseInt(versionString[count]);
								if(versionString[count]==null || versionString[count].equalsIgnoreCase("null") )// || keyArray[count].startsWith("key"))
									versionString[count] = 0+"";
								SimpleDynamoProvider.versionArray.put(keyArray[count], Integer.parseInt(versionString[count]));
								SimpleDynamoProvider.myLock.unlock();
							}
						}
						else
						{
							Log.e("Panic!!!! keyArray.length!=valueArray.length", "do something");
						}
						Log.e("Data updated Sucessfully", ":)");
					}
//					else
//					{
//						Log.e("RECOVERY_ACK string received Ignored", SimpleDynamoProvider.versionArray[Integer.parseInt(message.key)]+":"+message.key+":"+message.value+":"+message.versionNumber);
//					}
				}
				else if (message.messageType.equalsIgnoreCase(SimpleDynamoProvider.PUT1))
				{
					//if(message.versionNumber<=SimpleDynamoProvider.versionArray[Integer.parseInt(message.key)])
					{
						Log.v("PUT1 string received Adding, Myversion:key:value:MsgVersion", SimpleDynamoProvider.versionArray.get(message.key)+":"+message.key+":"+message.value+":"+message.versionNumber);
						//SimpleDynamoProvider.versionArray[Integer.parseInt(message.key)] = message.versionNumber;
						SimpleDynamoProvider.insertMessage(message.key, message.value, SimpleDynamoProvider.REQUESTOR_IS_COORDINATOR);
					}
					//else
					{
						//Log.e("Panic!!! Inconsistancy in msg received from requestor", "it is not possible!!!");
						//Log.e("Panic!!! PUT string received Ignored Myversion:key:value:MsgVersion", SimpleDynamoProvider.versionArray[Integer.parseInt(message.key)]+":"+message.key+":"+message.value+":"+message.versionNumber);
					}
				}
				else if(message.messageType.equalsIgnoreCase(SimpleDynamoProvider.PUT))
				{
					ObjectOutputStream oos = new ObjectOutputStream(listener.getOutputStream());
					message.messageType = SimpleDynamoProvider.PUT_ACK;
					oos.writeObject(message);
					oos.flush();
					//if(message.versionNumber<=SimpleDynamoProvider.versionArray[Integer.parseInt(message.key)])
					{
						Log.v("PUT string received Adding, Myversion:key:value:MsgVersion", SimpleDynamoProvider.versionArray.get(message.key)+":"+message.key+":"+message.value+":"+message.versionNumber);
						//SimpleDynamoProvider.versionArray[Integer.parseInt(message.key)] = message.versionNumber;
						SimpleDynamoProvider.insertMessage(message.key, message.value, SimpleDynamoProvider.REQUESTOR_IS_COORDINATOR);
					}
					//else
					{
						//Log.e("Panic!!! Inconsistancy in msg received from requestor", "it is not possible!!!");
						//Log.e("Panic!!! PUT string received Ignored Myversion:key:value:MsgVersion", SimpleDynamoProvider.versionArray[Integer.parseInt(message.key)]+":"+message.key+":"+message.value+":"+message.versionNumber);
					}
				}
				else if(message.messageType.equalsIgnoreCase(SimpleDynamoProvider.RECOVERY))
				{
					//if(message.versionNumber!=SimpleDynamoProvider.versionArray[0])
					if(SimpleDynamoProvider.versionArray.size()!=0)
					{
						//Log.v("RECOVERY string received Adding", SimpleDynamoProvider.versionArray[Integer.parseInt(message.key)]+":"+message.key+":"+message.value+":"+message.versionNumber);
						Cursor resultCursor = SimpleDynamoProvider.getDataForRecoveryAck();
						if(resultCursor!=null && resultCursor.getCount()>0)
						{
							resultCursor.moveToFirst();
							
							String keyArray = "";
							String valueArray = "";
							String versionString = "";
							while(true)
							{
								int keyIndex = resultCursor.getColumnIndex("key");
								int valueIndex = resultCursor.getColumnIndex("value");
								if (keyIndex == -1 || valueIndex == -1) 
								{
									Log.e("Error in LDump: ", "Wrong columns");
									resultCursor.close();
								}
								
								String tempKey = resultCursor.getString(keyIndex);
								keyArray = keyArray+tempKey+":";
								valueArray = valueArray+resultCursor.getString(valueIndex)+":";
								versionString = versionString+SimpleDynamoProvider.versionArray.get(tempKey)+":";
								
								if(resultCursor.isLast())
									break;
								else
									resultCursor.moveToNext();
							}
							resultCursor.close();
							keyArray = keyArray.substring(0, keyArray.length());
							valueArray = valueArray.substring(0, valueArray.length());
							versionString = versionString.substring(0, versionString.length());
												
							keyArray = keyArray + "#" + versionString;
							Message message2 = new Message(SimpleDynamoProvider.portStr, message.sender, 
									SimpleDynamoProvider.RECOVERY_ACK, keyArray, valueArray, 1);
							new MyClient().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message2);
							Log.e("RECOVERY_ACK send...", message2.key+":"+message2.value+":"+message2.versionNumber);
						}
					}
					else
					{
						//Log.e("Not replying to RECOVERY string MyVersion:key:val:MsgVersion", SimpleDynamoProvider.versionArray[0]+":"+message.key+":"+message.value+":"+message.versionNumber);
						Log.e("Not replying to RECOVERY string", "My verArray is empty");
					}
					
				}
				else if(message.messageType.equalsIgnoreCase(SimpleDynamoProvider.GET))
				{
//					Log.e("messageType.equals(GET", "let c");
//					Log.e("SizeofVersionArr", SimpleDynamoProvider.versionArray.size()+"");
//					Log.e("message.key:"+message.key, "contains?"+SimpleDynamoProvider.versionArray.containsKey(message.key));
//					if(SimpleDynamoProvider.versionArray.containsKey(message.key)==false)
//					{
//						Log.v("Key not found", message.key);
//					}
					//if((SimpleDynamoProvider.versionArray.containsKey(message.key)) && (message.versionNumber==SimpleDynamoProvider.versionArray.get(message.key)))
					if(SimpleDynamoProvider.versionArray.containsKey(message.key))
					{
						String value = SimpleDynamoProvider.getValue(message.key);
						if(value!=null)
						{
							message.value = value;
							message.receiver = message.sender;
							message.sender = SimpleDynamoProvider.portStr;
							message.messageType = SimpleDynamoProvider.GET_ACK;
							//new MyClient().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message);
							ObjectOutputStream oos = new ObjectOutputStream(listener.getOutputStream());
							oos.writeObject(message);
							oos.flush();
						}
						else
						{
							Log.e("Value==null for key:", message.key);
						}
					}
					else
					{
						//Log.e("Panic!!! Query Mismatch", SimpleDynamoProvider.versionArray.get(message.key)+":"+message.key+":"+message.versionNumber);
						Log.v("Key not found", message.key);
					}
				}
				/*else if(message.messageType.equalsIgnoreCase(SimpleDynamoProvider.GET_ACK))
				{
					SimpleDynamoProvider.queryResponseReceived = true;
					SimpleDynamoProvider.queryResponseMessage = message;
				}*/
				//ois.close();
				/*if(message.messageType.equalsIgnoreCase("put"))
				{
					SimpleDynamoProvider.insertMessage(message.key, message.value, "put");
					ObjectOutputStream oos = new ObjectOutputStream(listener.getOutputStream());
					message.receiver = message.sender;
					message.sender = SimpleDynamoProvider.portStr;
					
					message.messageType = "putAck";
					oos.writeObject(message);
					oos.close();
				}
				if(message.messageType.equalsIgnoreCase("replicate"))
				{
					SimpleDynamoProvider.insertMessage(message.key, message.value, "replicate");
					ObjectOutputStream oos = new ObjectOutputStream(listener.getOutputStream());
					message.receiver = message.sender;
					message.sender = SimpleDynamoProvider.portStr;
					
					message.messageType = "replicateAck";
					oos.writeObject(message);
					oos.close();
				}*/
			}//while ends

		}
		catch (IOException err) 
		{
			Log.e("IOException in Server:" , "err.getMessage()");
		} catch (ClassNotFoundException err) 
		{
			Log.e("ClassNotFoundException in server" ,"err.getMessage()");
		}
		return null;
	}
}

class MyClient extends AsyncTask<Message, Void, Void>
{
	@Override
	synchronized protected Void doInBackground(Message... messageArray) 
	{
		if(messageArray.length==2 && messageArray[0].messageType.equalsIgnoreCase(SimpleDynamoProvider.REPLICATE_STRING)
				&& messageArray[1].messageType.equalsIgnoreCase(SimpleDynamoProvider.REPLICATE_STRING))
		{
			for(int count=0; count<messageArray.length; count++)
			{
				try
				{
					final String tcpString = "10.0.2.2";
					int receiverPort = getReceiverPort(messageArray[count].receiver);
					Socket clientSocket = new Socket(tcpString, receiverPort);
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
					objectOutputStream.writeObject(messageArray[count]);
					clientSocket.close();
				}//try ends
				catch(IOException err)
				{
					Log.e(messageArray[count].receiver + " NOT ALIVE", "Error in sending replication msg");
				}
			}//for ends
		}//if ends
		else if (messageArray.length==1 && messageArray[0].messageType.equalsIgnoreCase(SimpleDynamoProvider.RECOVERY_ACK))
		{
			try
			{
				final String tcpString = "10.0.2.2";
				int receiverPort = getReceiverPort(messageArray[0].receiver);
				Socket clientSocket = new Socket(tcpString, receiverPort);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
				objectOutputStream.writeObject(messageArray[0]);
				clientSocket.close();
			}//try ends
			catch(IOException err)
			{
				Log.e("Problem!!! "+messageArray[0].receiver + " NOT ALIVE", "Error in sending RECOVERY_ACK msg");
			}
		}
		else if (messageArray.length==1 && messageArray[0].messageType.equalsIgnoreCase(SimpleDynamoProvider.PUT))
		{
			Socket clientSocket = null;
			try
			{
				final String tcpString = "10.0.2.2";
				int receiverPort = getReceiverPort(messageArray[0].receiver);
				clientSocket = new Socket(tcpString, receiverPort);
								
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
				objectOutputStream.writeObject(messageArray[0]);
				
				ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
				clientSocket.setSoTimeout(100);
				//Message message = (Message)objectInputStream.readObject();
				objectInputStream.readObject();
				clientSocket.close();
			}//try ends
			catch(SocketTimeoutException e)
			{
				Log.e("SocketTimeoutException", "Dont worry we are calling nodeDeadFwdToSuccessor");
				nodeDeadFwdToSuccessor(messageArray[0], clientSocket);
			}
			catch (IOException e) 
			{
				Log.e("IOException", "Dont worry we are calling nodeDeadFwdToSuccessor");
				nodeDeadFwdToSuccessor(messageArray[0], clientSocket);
			} 
			catch (ClassNotFoundException err) 
			{
				Log.e("Class NOt Found Exception", err.getMessage());
			}
			
		}
		else if(messageArray.length==1 && messageArray[0].messageType.equalsIgnoreCase(SimpleDynamoProvider.RECOVERY))
		{
			try
			{
				final String tcpString = "10.0.2.2";
				int receiverPort = getReceiverPort(messageArray[0].receiver);
				Socket clientSocket = new Socket(tcpString, receiverPort);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
				objectOutputStream.writeObject(messageArray[0]);
				objectOutputStream.flush();
				clientSocket.close();
			}
			catch (IOException err) 
			{
				Log.e("Problem!!! Sending Recovery msg to successor failed", messageArray[0].receiver+" NOT ALIVE");
			}
		}
		else if (messageArray.length==1 && messageArray[0].messageType.equalsIgnoreCase(SimpleDynamoProvider.GET))
		{
			Socket clientSocket = null;
			try
			{
				final String tcpString = "10.0.2.2";
				int receiverPort = getReceiverPort(messageArray[0].receiver);
				clientSocket = new Socket(tcpString, receiverPort);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
				objectOutputStream.writeObject(messageArray[0]);
				
				ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
				clientSocket.setSoTimeout(100);
				SimpleDynamoProvider.queryResponseMessage = (Message)objectInputStream.readObject();
				SimpleDynamoProvider.queryResponseReceived = true;
				clientSocket.close();
			}
			catch(SocketTimeoutException err)
			{
				Log.d("SocketTimeoutException GET msg to coordinator failed", messageArray[0].receiver+" NOT ALIVE");
				nodeDeadFwdToSuccessorGET(messageArray[0], clientSocket);
			}
			catch (IOException err) 
			{
				Log.d("IOException GET msg to coordinator failed", messageArray[0].receiver+" NOT ALIVE");
				nodeDeadFwdToSuccessorGET(messageArray[0], clientSocket);
			} 
			catch (ClassNotFoundException err) 
			{
				Log.e("ClassNotFoundException in MyClient.GET", "Help!!!");
			}
		}
		/*else if (messageArray.length==1 && messageArray[0].messageType.equalsIgnoreCase(SimpleDynamoProvider.GET_ACK))
		{
			try
			{
				final String tcpString = "10.0.2.2";
				int receiverPort = getReceiverPort(messageArray[0].receiver);
				Socket clientSocket = new Socket(tcpString, receiverPort);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
				objectOutputStream.writeObject(messageArray[0]);
				objectOutputStream.flush();
				clientSocket.close();
			}
			catch (IOException err) 
			{
				Log.e("Sending Get_Ack msg to coordinator failed", messageArray[0].receiver+" NOT ALIVE");
			}
		}*/
		return null;
		/*if(messageArray.length==1 && messageArray[0].messageType.equalsIgnoreCase("put"))
		{
			try
			{
				final String tcpString = "10.0.2.2";
				int receiverPort = getReceiverPort(messageArray[0].receiver);
								
				Socket clientSocket = new Socket(tcpString, receiverPort);
				clientSocket.setSoTimeout(100);
				
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
				objectOutputStream.writeObject(messageArray[0]);
				
				Message message = null;
				ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
				message = (Message)objectInputStream.readObject();
				
				clientSocket.close();
			}
			catch (SocketTimeoutException e)
			{
				Log.e("SocketTimeoutException, Error in Client1: ", messageArray[0].receiver +" NOT ALIVE");
			}
			catch(IOException e)
			{
				Log.e("IOException, Error in Client1: ", messageArray[0].receiver +" NOT ALIVE");
				SimpleDynamoProvider.insertMessage(messageArray[0].key, messageArray[0].value, "replicate");
				
				Message message = new Message();
				message.sender = SimpleDynamoProvider.portStr;
				message.receiver = getThirdNode(SimpleDynamoProvider.portStr, messageArray[0].receiver);
				message.messageType = "replicate";
				message.key = messageArray[0].key;
				message.value = messageArray[0].value;
				
				doInBackground(message);
			} 
			catch (ClassNotFoundException e) 
			{
				Log.e("ClassNotFoundException, Error in Client1: ", e.getMessage().toString());
			}

		}
		else if (messageArray[0].messageType.equalsIgnoreCase("replicate"))
		{
			int vote = 0 ;
			
			for(int count = 0; count<messageArray.length; count++)
			{
				try
				{
					final String tcpString = "10.0.2.2";
					int receiverPort = getReceiverPort(messageArray[count].receiver);
					Socket clientSocket = new Socket(tcpString, receiverPort);
					clientSocket.setSoTimeout(100);
					
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
					objectOutputStream.writeObject(messageArray[count]);
					
					Message message = null;
					ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
					message = (Message)objectInputStream.readObject();
					
					if(message!=null)
					{
						if(message.key.equalsIgnoreCase(messageArray[count].key) && message.value.equalsIgnoreCase(messageArray[count].value) && message.messageType.equalsIgnoreCase("replicateAck"))
							vote++;
					}
					
					clientSocket.close();
				}
				catch (SocketTimeoutException e)
				{
					Log.e("SocketTimeoutException, Error in Client2: ", e.getMessage());
				}
				catch(IOException e)
				{
					Log.e("IOException, Error in Client2: ", "IOException, Error in Client2");
				}
				catch (ClassNotFoundException e) 
				{
					Log.e("ClassNotFoundException, Error in Client2: ", e.getMessage().toString());
				}
			}
		}*/
	}
	
	synchronized public void nodeDeadFwdToSuccessor(Message message, Socket socket)
	{
		try
		{
			if(!socket.isClosed())
				socket.close();
			
			//SimpleDynamoProvider.insertMessage(message.key, message.value, SimpleDynamoProvider.JUST_ADD);
			String newDestination = getSuccessor(message.receiver);
			
			Message message2 = new Message(SimpleDynamoProvider.portStr, newDestination, SimpleDynamoProvider.PUT1,
					message.key, message.value, message.versionNumber);
			
			final String tcpString = "10.0.2.2";
			int receiverPort = getReceiverPort(message2.receiver);
			Socket clientSocket = new Socket(tcpString, receiverPort);
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
			objectOutputStream.writeObject(message2);
			
			//ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
			//Message message3 = (Message)objectInputStream.readObject();
			
			clientSocket.close();
		}
		catch (IOException err)
		{
			Log.e("Problem!!! IOException in nodeDeadFwdToSuccessor", err.getMessage());
		} 
		//catch (ClassNotFoundException err) 
		{
			//Log.e("ClassNotFoundException in nodeDeadFwdToSuccessor", err.getMessage());
		}
	}
	
	synchronized public void nodeDeadFwdToSuccessorGET(Message message, Socket socket)
	{
		try
		{
			if(!socket.isClosed())
				socket.close();
			
			String newDestination = getSuccessor(message.receiver);
			Message message2 = new Message(SimpleDynamoProvider.portStr, newDestination, SimpleDynamoProvider.GET,
					message.key, message.value, message.versionNumber);
			
			final String tcpString = "10.0.2.2";
			int receiverPort = getReceiverPort(message2.receiver);
			Socket clientSocket = new Socket(tcpString, receiverPort);
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
			objectOutputStream.writeObject(message2);
			
			ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
			clientSocket.setSoTimeout(100);
			SimpleDynamoProvider.queryResponseMessage = (Message)objectInputStream.readObject();
			SimpleDynamoProvider.queryResponseReceived = true;
			
			clientSocket.close();
		}
		catch(SocketTimeoutException err)
		{
			
		}
		catch(IOException err)
		{
			
		} 
		catch (ClassNotFoundException err) 
		{
		
		}
	}
	
	synchronized public int getReceiverPort(String receiver)
	{
		int receiverPort = 0;
		
		if(receiver.equals("5554"))
			receiverPort = 11108;
		else if (receiver.equals("5556"))
			receiverPort = 11112;
		else if (receiver.equals("5558"))
			receiverPort = 11116;
		
		return receiverPort;
	}
	
	synchronized public String getThirdNode(String portStr, String failedNode)
	{
		HashMap<String, String> hashMap = new HashMap<String, String>();
		hashMap.put("5554", "5554");
		hashMap.put("5556", "5556");
		hashMap.put("5558", "5558");
		
		hashMap.remove(portStr);
		hashMap.remove(failedNode);
		
		if(hashMap.size()==1)
		{
			Iterator<String> iterator = hashMap.keySet().iterator();
			String thirdNode = (String)iterator.next();
			return thirdNode;
		}
		else
			return null;
	}
	
	synchronized public String getSuccessor(String portStr)
	{
		if(portStr.equalsIgnoreCase("5554"))
			return "5558";
		else if(portStr.equalsIgnoreCase("5558"))
			return "5556";
		else if (portStr.equalsIgnoreCase("5556"))
			return "5554";
		
		return null;
	}
	
}

@SuppressWarnings("serial")
class Message implements Serializable
{
	public String sender;
	public String receiver;
	
	public String messageType;
	public String key;
	public String value;
	
	public Integer versionNumber;
	
	public Message(String sender, String receiver, String messageType, String key, String value, int versionNumber) 
	{
		super();
		this.sender = sender;
		this.receiver = receiver;
		this.messageType = messageType;
		this.key = key;
		this.value = value;
		this.versionNumber = versionNumber;
	}
}

/*String hash5556 = genHash("5556");//genHash("5556")=208f7f72b198dadd244e61801abe1ec3a4857bc9
String hash5554 = genHash("5554");//genHash("5554")=33d6357cfaaf0f72991b0ecd8c56da066613c089
String hash5558 = genHash("5558");//genHash("5558")=abf0fd8db03e5ecb199a9b82929e9db79b909643
*/
//Log.v("keyHash", keyHash);
			
/*if(keyHash.compareTo(hash5556)>0 && key.compareTo(hash5554)<=0)
	return "5554";
else if(keyHash.compareTo(hash5554)>0 && keyHash.compareTo(hash5558)<=0)
	return "5558";
else
	return "5556";*/

/*if(keyHash.compareTo(hash5554)<0)
	return "5556";
if(keyHash.compareTo(hash5554)>=0 && keyHash.compareTo(hash5558)<0)
	return "5554";
else
	return "5558";*/