/**
 * Copyright 2012-2013 StackMob
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stackmob.customcode;

import com.stackmob.core.customcode.CustomCodeMethod;
import com.stackmob.core.rest.ProcessedAPIRequest;
import com.stackmob.core.rest.ResponseToProcess;
import com.stackmob.sdkapi.SDKServiceProvider;
import com.stackmob.core.DatastoreException;
import com.stackmob.core.InvalidSchemaException;
import com.stackmob.sdkapi.*;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetPersonWakingUp implements CustomCodeMethod {

  @Override
  public String getMethodName() {
    return "get_person_waking_up";
  }

  @Override
  public List<String> getParams() {
    ArrayList<String> params = new ArrayList<String>();
	params.add("personId");//person ID
	params.add("time");//timeSince1970
	params.add("location");//"111.1, 12.6"
	return params;
  }

  @Override
  public ResponseToProcess execute(ProcessedAPIRequest request, SDKServiceProvider serviceProvider) {
    DataService ds = serviceProvider.getDataService();
    LoggerService logger = serviceProvider.getLoggerService(GetPersonWakingUp.class);
    
    String personId = request.getParams().get("person");
    Long time = Long.parseLong(request.getParams().get("time"));
    String[] location = request.getParams().get("location").split(",");
    if(personId == null || time == null || location == null){
    	logger.error("Empty inputs, check your code");
    	HashMap<String, String> errMap = new HashMap<String, String>();
	      errMap.put("error", "invalid_input");
	      errMap.put("detail", "Check your input");
    	return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
    }
    logger.info("location is" + location[0] + ", " + location[1]);
    logger.info("time is " + String.valueOf(time));
    logger.info("person is" + personId);
    
    //data
    //Calendar now = Calendar.getInstance();
    //now.add(Calendar.MINUTE, 10);
    Long time2 = time + 600;                                 
    //query
    List<SMCondition> timeQuery = new ArrayList<SMCondition>();
    List<SMCondition> personQuery = new ArrayList<SMCondition>();
	List<SMObject> results = new ArrayList<SMObject>();
	List<SMObject> results2 = new ArrayList<SMObject>();
	
	
	try{
		//get tasks with in 10 min of given time
		timeQuery.add(new SMGreaterOrEqual("time", new SMInt(time)));
		timeQuery.add(new SMLessOrEqual("time", new SMInt(time2)));
		results = ds.readObjects("ewtask", timeQuery);
		logger.info(String.valueOf(results.size()) + "tasks has been found");
		//condition 1: query person that has that task
		personQuery.add(new SMIn("task", results));
		//condition 2: get location
		SMNear near = new SMNear("location", Long.parseLong(location[0]), Long.parseLong(location[1]), 0.25);
		personQuery.add(near);
		//fetch
		ResultFilters filters = new ResultFilters(0, 50, null, null);
		results2 = ds.readObjects("ewperson", personQuery, 1, filters);
		logger.info(String.valueOf(results2.size()) + "person has been queried");
		
		
	}catch (InvalidSchemaException e) {
	      HashMap<String, String> errMap = new HashMap<String, String>();
	      errMap.put("error", "invalid_schema");
	      errMap.put("detail", e.toString());
	      return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap); // http 500 - internal server error
	    } catch (DatastoreException e) {
	      HashMap<String, String> errMap = new HashMap<String, String>();
	      errMap.put("error", "datastore_exception");
	      errMap.put("detail", e.toString());
	      return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap); // http 500 - internal server error
	    } catch(Exception e) {
	      HashMap<String, String> errMap = new HashMap<String, String>();
	      errMap.put("error", "unknown");
	      errMap.put("detail", e.toString());
	      return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap); // http 500 - internal server error
	    }
	
	Map<String, Object> map = new HashMap<String, Object>();
	map.put("person", results2);
    return new ResponseToProcess(HttpURLConnection.HTTP_OK, map);
  }
}
