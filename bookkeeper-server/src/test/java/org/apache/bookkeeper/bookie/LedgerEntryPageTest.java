package org.apache.bookkeeper.bookie;


import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;

/************************************************
 * 
 * @author marco
 *
 *	Simple test case aimed to show Mockito utility
 *	not for coverage study
 *
 ************************************************/

@RunWith(value=Parameterized.class)
public class LedgerEntryPageTest{
	
	private int pageSize;
	private int entriesPerPage;
	private LedgerEntryPage lepCall;
	private int repetitions;

	
	private static Random generator;
	
	LEPStateChangeCallback callback;
	
	@Parameters
	public static Collection<Object[]> params(){
		return Arrays.asList( new Object[][]{
				//page size, entriesPerPage
				{5,2}
		});
	}
	
	public LedgerEntryPageTest(int pageSize, int entriesPerPage) {
		this.pageSize = pageSize;
		this.entriesPerPage = entriesPerPage;
	}
	
	@BeforeClass
	public static void configureGenerator() {
		generator = new Random();
	} 
	
	
	@Before
	public void configure() {
			//min value 1
			this.repetitions = generator.nextInt(50)+1;
			
			//configure callback lep
			this.callback = Mockito.mock(LEPStateChangeCallback.class); 
			this.lepCall = new LedgerEntryPage(this.pageSize, this.entriesPerPage, this.callback);
			//configure behavior
			Mockito.doNothing().when(this.callback).onResetInUse(Mockito.isA(LedgerEntryPage.class));
			Mockito.doNothing().when(this.callback).onSetInUse(Mockito.isA(LedgerEntryPage.class));
			
	}
	
	
	@Test
	public void testOnSetInUseCallback() {
		//use page with a callback
		for(int i=0; i<this.repetitions; i++) {
			this.lepCall.usePage();
			this.lepCall.resetPage();
		}
		
		Mockito.verify(callback, Mockito.times(repetitions)).onSetInUse(lepCall);
	}
	
	
	
	
	@After
	public void resetPage() throws Exception {
		//close page
		if(this.lepCall.inUse())
			this.lepCall.close();
	}
	
	
	
	
}
