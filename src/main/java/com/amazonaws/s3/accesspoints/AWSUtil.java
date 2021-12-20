// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.s3.accesspoints;

import software.amazon.awssdk.regions.Region;

public class AWSUtil {

	public static Region getRegion(String regionString) {
		Region region = Region.regions().stream().filter(r -> r.toString().equalsIgnoreCase(regionString)).findFirst()
				.orElse(Region.US_EAST_1);
		return region;
	}

}
