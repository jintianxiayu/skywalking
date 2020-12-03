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

package org.apache.skywalking.apm.plugin.httpasyncclient.v4;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.RequestLine;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.skywalking.apm.agent.core.context.CarrierItem;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;
import org.apache.skywalking.apm.plugin.httpasyncclient.v4.wrapper.HttpAsyncResponseConsumerWrapper;

/**
 * in main thread,hold the context in thread local so we can read in the same
 * thread.
 */
public class HttpAsyncClientInterceptor implements InstanceMethodsAroundInterceptor {

	@Override
	public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
			MethodInterceptResult result) throws Throwable {

		HttpAsyncRequestProducer requestProducer = (HttpAsyncRequestProducer) allArguments[0];
		if (requestProducer instanceof BasicAsyncRequestProducer) {

			// 对于在一个线程中多次发送http请求时会创建多个ExitSpan，但只有第一个会生效，其他被忽略，导致span丢失
			// 所以这里对于每次请求先创建一个LocalSpan作为每个ExitSpan的父span来延续多个ExitSpan
			AbstractSpan localSpan = ContextManager.createLocalSpan("httpasyncclient/local");
			
			// 因为异步原因，span的finish（asyncFinish）操作会在其他线程中进行；下面的exitspan同理
			localSpan.prepareForAsync();

			BasicAsyncRequestProducer brp = (BasicAsyncRequestProducer) requestProducer;
			HttpRequest request = brp.generateRequest();
			
			RequestLine requestLine = request.getRequestLine();
			String uri = requestLine.getUri();
			String operationName = uri.startsWith("http") ? new URL(uri).getPath() : uri;
			HttpHost httpHost = requestProducer.getTarget();
			int port = httpHost.getPort();
			
			final ContextCarrier contextCarrier = new ContextCarrier();
			AbstractSpan exitSpan = ContextManager.createExitSpan(operationName, contextCarrier,
					httpHost.getHostName() + ":" + (port == -1 ? 80 : port));
			exitSpan.prepareForAsync();

			exitSpan.setComponent(ComponentsDefine.HTTP_ASYNC_CLIENT);
			Tags.URL.set(exitSpan, requestLine.getUri());
			Tags.HTTP.METHOD.set(exitSpan, requestLine.getMethod());
			SpanLayer.asHttp(exitSpan);
			CarrierItem next = contextCarrier.items();
			while (next.hasNext()) {
				next = next.next();
				request.setHeader(next.getHeadKey(), next.getHeadValue());
			}

			HttpAsyncResponseConsumer<?> consumer = (HttpAsyncResponseConsumer<?>) allArguments[1];
			// 上面创建的exitSpan和localSpan的finish操作会在下面的回调中执行，直接将span传过去
			allArguments[1] = new HttpAsyncResponseConsumerWrapper<>(consumer, Arrays.asList(exitSpan, localSpan));

			// exitSpan
			ContextManager.stopSpan();
			// localSpan
			ContextManager.stopSpan();

		}

	}

	@Override
	public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
			Object ret) throws Throwable {
		return ret;
	}

	@Override
	public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
			Class<?>[] argumentsTypes, Throwable t) {

	}
}
