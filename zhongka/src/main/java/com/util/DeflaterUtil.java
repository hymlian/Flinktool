package com.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.commons.codec.binary.Base64;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;


/**
 * 
 * @author huyunmian
 *
 */
public class DeflaterUtil {

	public static String  compress(String message) throws IOException {
		// Compression level BEST_SPEED
		Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION);
		deflater.setInput(message.getBytes("UTF-8"));

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(message.getBytes().length);
		// When finish() called, indicates that compression should end
		// with the current contents of the input buffer.
		deflater.finish();
		byte[] buffer = new byte[1024];
		// finished() returns true if the end of the compressed data
		// output stream has been reached.
		while (!deflater.finished()) {
			int count = deflater.deflate(buffer);
			outputStream.write(buffer, 0, count);
		}
		// Closes the compressor and discards any unprocessed input.
		deflater.end();

		// Covert to Base64
		String compressedMessage = Base64.encodeBase64String(outputStream.toByteArray());
		outputStream.close();

		return new String(compressedMessage.getBytes());
	}
	public static List<String> unCompressed(String msg) throws Exception {
		byte[] input = Base64.decodeBase64(msg);
		ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);

		Inflater decompressor = new Inflater();

		try {
			decompressor.setInput(input);

			byte[] buf = new byte[1024];
			while (!decompressor.finished()) {
				int count = decompressor.inflate(buf);
				bos.write(buf, 0, count);
			}
			buf = null;
		} finally {
			decompressor.end();
		}

		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		BufferedReader br = new BufferedReader(new InputStreamReader(bis));
		ArrayList<String> result = new ArrayList<String>();
		String value = null;
		try {
			while ((value = br.readLine()) != null) {
				result.add(value);
			}
		} finally {
			bos.close();
			bis.close();
			br.close();
		}

		return result;
	}

}
