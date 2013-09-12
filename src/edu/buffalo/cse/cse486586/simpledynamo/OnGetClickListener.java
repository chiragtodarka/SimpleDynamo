package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class OnGetClickListener implements OnClickListener 
{
	public TextView textView;
	public ContentResolver contentResolver;
	public Uri uri;
	
	public OnGetClickListener(TextView textView, ContentResolver contentResolver)
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
		protected Void doInBackground(Void... params) 
		{
			for(int count=0; count<OnPutClickListener.TEST_COUNT; count++)
			{
				Cursor resultCursor = contentResolver.query(uri, null, ""+count, null, null);
				if(resultCursor==null || resultCursor.getCount()<=0)
					publishProgress("Query for "+ count + " failed\n");
				else
				{
					int keyIndex = resultCursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_KEY);
					int valueIndex = resultCursor.getColumnIndex(SimpleDynamoDatabase.COLUMN_VALUE);
					resultCursor.moveToFirst();
					String returnKey = resultCursor.getString(keyIndex);
					String returnValue = resultCursor.getString(valueIndex);
					publishProgress(returnKey+returnValue+"\n");
				}
			}
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
