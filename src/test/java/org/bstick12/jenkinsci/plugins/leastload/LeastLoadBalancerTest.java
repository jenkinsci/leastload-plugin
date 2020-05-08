/*
 * The MIT License
 * 
 * Copyright (c) 2013, Brendan Nolan
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.bstick12.jenkinsci.plugins.leastload;

import static org.junit.Assert.assertEquals;
import hudson.model.LoadBalancer;
import hudson.model.AbstractProject;
import hudson.model.Queue.Task;
import hudson.model.queue.MappingWorksheet;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * 
 * @author brendan.nolan@gmail.com
 *
 */
public class LeastLoadBalancerTest {

	protected LoadBalancer mockFallback = Mockito.mock(LoadBalancer.class);
	
	@SuppressWarnings("rawtypes")
	protected AbstractProject mockAbstractProject = Mockito.mock(AbstractProject.class);
	
	protected MappingWorksheet mockMappingWorksheet = Mockito.mock(MappingWorksheet.class);
	
	@Test(expected=NullPointerException.class)
	public void testConstructorFailure() {
		new LeastLoadBalancer(null);
	}
	
	@Test
	public void testConstructor() {
		LeastLoadBalancer llb = new LeastLoadBalancer(mockFallback);
		assertEquals(mockFallback, llb.getFallBackLoadBalancer());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testFallbackOnFailure() {
		LeastLoadBalancer llb = new LeastLoadBalancer(mockFallback);
		LeastLoadDisabledProperty lldp = new LeastLoadDisabledProperty(false);
		Mockito.doReturn(lldp).when(mockAbstractProject).getProperty(LeastLoadDisabledProperty.class);		
		llb.map(mockAbstractProject, mockMappingWorksheet);
		Mockito.verify(mockFallback).map(mockAbstractProject, mockMappingWorksheet);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testFallbackOnPropertyDisable() {
		
		LeastLoadBalancer llb = new LeastLoadBalancer(mockFallback);		
		LeastLoadDisabledProperty lldp = new LeastLoadDisabledProperty(true);
		Mockito.doReturn(lldp).when(mockAbstractProject).getProperty(LeastLoadDisabledProperty.class);		
		llb.map(mockAbstractProject, mockMappingWorksheet);
		Mockito.verify(mockFallback).map(mockAbstractProject, mockMappingWorksheet);
		
	}
	
	@Test
	public void testFallbackWhenNotAbstractProject() {
		
		LeastLoadBalancer llb = new LeastLoadBalancer(mockFallback);
		Task mockTask = Mockito.mock(Task.class);
		llb.map(mockTask, mockMappingWorksheet);
		Mockito.verify(mockFallback).map(mockTask, mockMappingWorksheet);
		
	}
	
}