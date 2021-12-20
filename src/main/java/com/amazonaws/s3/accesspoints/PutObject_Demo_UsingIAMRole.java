// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.s3.accesspoints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class PutObject_Demo_UsingIAMRole {

	public static void main(String[] args) {

		String bucket = args[0];
		String key = args[1];
		String regionString = args[2];

		S3Client s3 = S3Client.builder().region(AWSUtil.getRegion(regionString)).build();

		// Put Object
		try {
			s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
					RequestBody.fromByteBuffer(getRandomByteBuffer(10_000)));
		} catch (AwsServiceException | SdkClientException | IOException e) {
			e.printStackTrace();
		}

	}

	private static ByteBuffer getRandomByteBuffer(int size) throws IOException {
		byte[] b = new byte[size];
		new Random().nextBytes(b);
		return ByteBuffer.wrap(b);
	}

}
