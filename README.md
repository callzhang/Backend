stackmob-customcode-java-starter
================================

## Overview

Custom Code Starter project that has all Maven files, folder structures, and EntryPointExtender.java ready.  

## Run

You can get your code running on StackMob in two ways:

* running via a JAR
* running via GitHub

### Running via a JAR

To build a JAR, run `mvn package` from the root folder.  You can then <a href="https://dashboard.stackmob.com/module/customcode/upload" target="_blank">Upload your JAR to StackMob</a>, where we'll load your method into your Development environment automatically.

### Running via GitHub

If you fork this repository, you can <a href="https://dashboard.stackmob.com/module/customcode/upload_github" target="_blank">link your Custom Code GitHub repository to StackMob</a>.  StackMob will build your project for you and load your methods into your Development environment automatically.

## Moving EntryPointExtender.java

Did you change your Java package? (`com.myproject.customcode`).   Be sure you update `pom.xml` with `EntryPointExtender.java`'s location.

Update `<mainClass>com.stackmob.customcode.EntryPointExtender</mainClass>`  (line 78) in your `pom.xml` to the appropriate package.
