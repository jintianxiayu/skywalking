/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.skywalking.apm.plugin.httpasyncclient.v4.wrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpContext;
import org.apache.skywalking.apm.agent.core.context.AsyncSpan;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.ExitSpan;

/**
 * a wrapper for {@link HttpAsyncResponseConsumer} so we can be notified when the current response(every response will
 * callback the wrapper) received maybe completed or canceled,or failed.
 */
public class HttpAsyncResponseConsumerWrapper<T> implements HttpAsyncResponseConsumer<T> {

    private HttpAsyncResponseConsumer<T> consumer;
	private List<AsyncSpan> spans = new ArrayList<>();

	public HttpAsyncResponseConsumerWrapper(HttpAsyncResponseConsumer<T> consumer, List<AsyncSpan> spans) {
		this.consumer = consumer;
		if (spans != null) {
			this.spans = spans;
		}
	}

    @Override
    public void responseReceived(HttpResponse response) throws IOException, HttpException {
		int statusCode = response.getStatusLine().getStatusCode();
		for (AsyncSpan span : spans) {
			span.asyncFinish();
			if (statusCode >= 400 && span instanceof ExitSpan) {
				ExitSpan exitSpan = (ExitSpan) span;
				exitSpan.errorOccurred();
				Tags.STATUS_CODE.set(exitSpan, String.valueOf(statusCode));
			}
		}
        consumer.responseReceived(response);
    }

    @Override
    public void consumeContent(ContentDecoder decoder, IOControl ioctrl) throws IOException {
        consumer.consumeContent(decoder, ioctrl);
    }

    @Override
    public void responseCompleted(HttpContext context) {
        consumer.responseCompleted(context);
    }

    @Override
    public void failed(Exception ex) {
		for (AsyncSpan span : spans) {
			span.asyncFinish();
			if (span instanceof ExitSpan) {
				ExitSpan exitSpan = (ExitSpan) span;
				exitSpan.errorOccurred().log(ex);
			}
		}
        consumer.failed(ex);

    }

    @Override
    public Exception getException() {
        return consumer.getException();
    }

    @Override
    public T getResult() {
        return consumer.getResult();
    }

    @Override
    public boolean isDone() {
        return consumer.isDone();
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }

    @Override
    public boolean cancel() {
		for (AsyncSpan span : spans) {
			span.asyncFinish();
			if (span instanceof ExitSpan) {
				ExitSpan exitSpan = (ExitSpan) span;
				exitSpan.errorOccurred();
			}
		}
        return consumer.cancel();
    }
}
