package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class SimpleDynamoDatabase extends SQLiteOpenHelper
{
	public static final String DATABASE_NAME = "SimpleDynamoDatabase.db";
	public static final int DATABASE_VERSION = 1;
	
	public static final String TABLE_NAME = "SimpleDynamoTable";
	public static final String COLUMN_KEY = "key";
	public static final String COLUMN_VALUE = "value";
	
	public static final String TABLE_CREATION_STRING = "create table " + TABLE_NAME
			+ "("+ COLUMN_KEY + " text primary key,"
			+ COLUMN_VALUE +" text);" ;
	
	public SimpleDynamoDatabase(Context context) 
	{
		super(context, DATABASE_NAME, null, DATABASE_VERSION);
	}

	@Override
	public void onCreate(SQLiteDatabase sqliteDatabase) 
	{
		sqliteDatabase.execSQL("DROP TABLE IF EXISTS '" + TABLE_NAME +"'");
		sqliteDatabase.execSQL(TABLE_CREATION_STRING);
	}

	@Override
	public void onUpgrade(SQLiteDatabase sqliteDatabase, int oldVersion, int newVersion) 
	{
		Log.v("SimpleDynamoDatabase",
			      "Upgrading database from version " + oldVersion + " to "
			      + newVersion + ", which will destroy all old data");
		
		sqliteDatabase.execSQL("DROP TABLE IF EXISTS '" + TABLE_NAME +"'");
		sqliteDatabase.execSQL(TABLE_CREATION_STRING);
	}
}
