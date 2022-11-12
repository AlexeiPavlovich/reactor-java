package com.example;



import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.context.Context;
import reactor.util.context.ContextView;



//extends Subscriber<T>, Publisher<R>
public class ReactorTests {
	
	static interface Common{
		default void addGateway(String str) {}
		default String getGateway() {return null;}
		default ContextView getContext() {return null;}
	}
	
	static class Entity implements Common{
		
		@Override
		public String toString() {
			String contextStr=(context==null) ? null : "[id="+((Entity)context.get("entity")).id+" gw="+((Entity)context.get("entity")).gw.toString()+"]";
			return "Entity [id=" + id + ", gw=" + gw.toString() + ", acceptLanguage=" + acceptLanguage+ ", context=" + contextStr + "]";
		}


		public Entity() {

		}
		public Entity(ContextView context) {
			this.context=context;
		}
		

		@Override
		public void addGateway(String str) {
			gw.add(str);
		}
		
		@Override
		public ContextView getContext() {
			return context;
		}
		int id;
		List<String> gw=new ArrayList<>();
		String acceptLanguage;
		
		ContextView context;
	}
	
	
	static class Gw implements Common{
		@Override
		public String toString() {
			String contextStr=(context==null) ? null : context.get("entity").toString();
			return "Gw [gateway=" + gateway + ", context=[" + contextStr + "]]";
		}
		public Gw(ContextView context, String gateway) {
			this.context = context;
			this.gateway = gateway;
		}
		String gateway;
		ContextView context;
		
		@Override
		public ContextView getContext() {
			return context;
		}
		
		@Override
		public String getGateway() {
			return gateway;
		}
	}
	//o1 always Entity
	Common mapGateway(Common o1, Common o2) {
		//System.out.println("+++++++ "+o1);
		Entity entity=o1.getContext().get("entity");
		Stream.of(o1,o2).map(i->i.getGateway()).filter(Objects::nonNull).forEach(entity::addGateway);
		return entity;
	}

	Mono<Entity> service(ContextView context, Mono<Entity> main, List<Mono<Gw>> gateways) {
		List<Mono<? extends Common>> lst = new ArrayList<>();
		lst.add(main);
		lst.addAll(gateways);
		return Flux.mergeSequential(lst).contextWrite(context).parallel(lst.size()).runOn(Schedulers.parallel())
				.doOnNext(i -> System.out.println(i + " " + Thread.currentThread().getName())).sequential()
				.reduce(this::mapGateway).map(c -> c.getContext().get("entity"));
	}
	
	Mono<Gw> createGwWithContext(String gatewayStr){
		return Mono.deferContextual(ctx -> Mono.just(new Gw(ctx, gatewayStr)));
	}
	
	Mono<Entity> createEntityWithContext(Entity entity) {
		return Mono.deferContextual(ctx -> {
			entity.context = ctx;
			Mono<Entity> m = Mono.just(entity);
			return m;
		});
	}

	@Test
	public void testService() throws InterruptedException {

		Function<Entity, Entity> setId = d -> {
			d.id = 5;
			return d;
		};

		Function<Entity, Entity> setAcceptLanguage = d -> {
			d.acceptLanguage = "en-US";
			return d;
		};

		Function<Entity, Mono<Entity>> initService = entity -> {

			ContextView context = Context.of("entity", entity, "acceptLanguage", "en-US");

			Mono<Gw> gateway1 = createGwWithContext("response A").delayElement(Duration.ofSeconds(2));
			Mono<Gw> gateway2 = createGwWithContext("response B").delayElement(Duration.ofSeconds(3));
			Mono<Gw> gateway3 = Mono.just("response C").flatMap(this::createGwWithContext)
					.delayElement(Duration.ofSeconds(1));
			Mono<Entity> main = createEntityWithContext(entity).delayElement(Duration.ofSeconds(5)).map(setId)
					.map(setAcceptLanguage);

			return service(context, main, Arrays.asList(gateway1, gateway2, gateway3));
		};

		initService.apply(new Entity())
				.subscribe(i -> System.out.println("response: " + i + " " + Thread.currentThread().getName()));

		
		
		Entity ent = new Entity();
		Mono<Entity> response = initService.apply(ent);

		StepVerifier.create(response).expectNextMatches(e -> ent==e && e.gw.size() == 3).verifyComplete();

	}
	

	@Test
	public void fluxCreateTest() {
		Flux<Integer> service1=Flux.just(1, 2, 3, 4, 5);
		
		Flux.create(sink->service1.subscribe(new BaseSubscriber<Integer>() {
			@Override
			protected void hookOnNext(Integer value) {
				sink.next(value);
			}
			@Override
			protected void hookOnComplete() {
				super.onComplete();
			}
		})).subscribe(System.out::println);
		
		
		
		Flux.create(sink->sink.onRequest(r->{
			sink.next("response from gw");
		})).subscribe(System.out::println);
		
	}
	

	
	@Test
	public void test() {
		List<Integer> elements = new ArrayList<>();

		Flux.just(1, 2, 3, 4)
		  .log()
		  .subscribe(elements::add);

		assertThat(elements).containsExactly(1, 2, 3, 4);
		
		
		Flux.just(1, 2, 3, 4)
		  .log()
		  .subscribe(new Subscriber<Integer>() {
		    @Override
		    public void onSubscribe(Subscription s) {
		      s.request(Long.MAX_VALUE);
		    }

		    @Override
		    public void onNext(Integer integer) {
		      elements.add(integer);
		    }

		    @Override
		    public void onError(Throwable t) {}

		    @Override
		    public void onComplete() {}
		});
		
		
	}
	


	@Test
	public void test2() {
		List<String> elements = new ArrayList<>();
		Flux.just(1, 2, 3, 4)
		  .log()
		  .map(i -> i * 2)
		  .zipWith(Flux.range(0, Integer.MAX_VALUE), 
		    (one, two) -> String.format("First Flux: %d, Second Flux: %d", one, two))
		  .subscribe(elements::add);
		
		
		assertThat(elements).containsExactly(
				  "First Flux: 2, Second Flux: 0",
				  "First Flux: 4, Second Flux: 1",
				  "First Flux: 6, Second Flux: 2",
				  "First Flux: 8, Second Flux: 3");
		
		
		}
	
	@Test
	public void test3() {
		ConnectableFlux<Object> publish = Flux.create(fluxSink -> {
		    for(int i=0;i<5;i++) {
		        fluxSink.next(System.currentTimeMillis());
		    }
		}).sample(Duration.ofSeconds(1)).publish();	
		
		publish.subscribe(System.out::println);        
		publish.subscribe(System.out::println);
		publish.connect();
		
	}



	/*@Test
	public void test4() {
		List<Integer> elements = new ArrayList<>();

		
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(50);

		executor.setThreadNamePrefix("SecurityThread-");
		executor.initialize();
		TaskExecutor te= new DelegatingSecurityContextAsyncTaskExecutor(executor);
		
		

		Flux.range(0,20)
		  //.log()
		  .map(i -> i * 2)
		  .parallel(10)
		  .runOn(Schedulers.fromExecutor(te))
		  //.log()
		  .subscribe(new Subscriber<Integer>() {
			  Subscription s;
			  @Override
			    public void onSubscribe(Subscription s) {
			      this.s=s;
				  s.request(1);
			    }
			 
			    @Override
			    public void onNext(Integer integer) {
			     System.out.println(integer+" "+Thread.currentThread().getName()); 
	
			     elements.add(integer);
			     s.request(1);
			    }
			 
			    @Override
			    public void onError(Throwable t) {}
			 
			    @Override
			    public void onComplete() {}
			});
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		executor.shutdown();
	}

*/

	@Test
	public void test5() {
		final String id="12345";
		
		Supplier<User> supplier = () ->{
			System.out.println(id+" flux:callable task executor: " + Thread.currentThread().getName());
			User u=new User();
			u.setId(id);

			return u;
		};
		
		
		Mono<User> test=Mono.fromSupplier(supplier);
		
		test.subscribe(response->System.out.println(response + " task executor: " + Thread.currentThread().getName()));
		
		
		test=test.publishOn(Schedulers.elastic());
		test.subscribe(response->System.out.println(response.getId() + " task executor: " + Thread.currentThread().getName()));
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}


}
