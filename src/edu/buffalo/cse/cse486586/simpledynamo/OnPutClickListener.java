package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;
import android.content.ContentValues;

public class OnPutClickListener implements OnClickListener
{
	static int TEST_COUNT = 20;
	
	public TextView textView;
	public ContentResolver contentResolver;
	public Uri uri;
	public String putString;
	
	public OnPutClickListener(TextView textView, ContentResolver contentResolver, String putString) 
	{
		this.textView = textView;
		this.contentResolver = contentResolver;
		this.uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		this.putString = putString;
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
		new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, this.putString);
	}
	
	private class Task extends AsyncTask<String, String, Void>
	{
		@Override
		protected Void doInBackground(String... putStrings) 
		{
			for(int count =0; count<TEST_COUNT; count++)
			{
				ContentValues contentValues = new ContentValues();
				String key = count + "";
				String value = putStrings[0] + count;
				contentValues.put("key", key);
				contentValues.put("value", value);
				
				contentResolver.insert(uri, contentValues);
				try 
				{
					Thread.sleep(1000);
				} 
				catch (InterruptedException e) 
				{
					publishProgress("Insert fail\n");
					Log.e("InterruptedException in OnPutClickListener.Task", e.getMessage());
				}
			}
			publishProgress("Insert success\n");
			return null;
		}
		
		@Override
		protected void onProgressUpdate(String...strings) 
		{
			textView.append(strings[0]);
			return;
		}

	}
}