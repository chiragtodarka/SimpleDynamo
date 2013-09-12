package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class OnLDumpClickListener implements OnClickListener 
{
	public TextView textView;
	public ContentResolver contentResolver;
	public Uri uri;
	
	public OnLDumpClickListener(TextView textView, ContentResolver contentResolver) 
	{
		this.textView = textView;
		this.contentResolver = contentResolver;
		this.uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	}
	
	private Uri buildUri(String scheme, String authority) 
	{
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	
	@Override
	public void onClick(View view) 
	{
		new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
	}
	
	private class Task extends AsyncTask<Void, String, Void> 
	{
		@Override
		protected Void doInBackground(Void... arg0) 
		{
			String data = "";
			Cursor resultCursor = contentResolver.query(uri, null, "LDUMP", null, null);
			
			if (resultCursor == null) 
			{
				Log.e("Error in LDump: ", "Result null");
				data = "No data in DHT\n";
			}
			else
			{
				if(resultCursor.getCount() != 0)
				{
					resultCursor.moveToFirst();
					
					while(true)
					{
						int keyIndex = resultCursor.getColumnIndex("key");
						int valueIndex = resultCursor.getColumnIndex("value");
						if (keyIndex == -1 || valueIndex == -1) 
						{
							Log.e("Error in LDump: ", "Wrong columns");
							resultCursor.close();
						}
						
						String returnKey = resultCursor.getString(keyIndex);
						String returnValue = resultCursor.getString(valueIndex);
						
						data = data + returnKey + returnValue + "\n";
						
						if(resultCursor.isLast())
						{
							break;
						}
						else
						{
							resultCursor.moveToNext();
						}
					}
				}
				else
				{
					data = "No data in DHT\n";
				}
				
				resultCursor.close();
			}
			//for(int count=0; count<OnPutClickListener.TEST_COUNT; count++)
			{
				//data = data + "key" + count + ":Version" + SimpleDynamoProvider.versionArray.get(count+"")+"\n";
			}
			publishProgress(data);
			return null;
		}
		
		@Override
		protected void onProgressUpdate(String... strings) 
		{
			textView.append(strings[0]);
			return;
		}
		
	}

}
