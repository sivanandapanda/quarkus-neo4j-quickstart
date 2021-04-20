package com.example.datasource;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Values;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.NoSuchRecordException;

import java.net.URI;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

@Path("fruits")
public class FruitResource {

    @Inject
    Driver driver;

    @GET
    public CompletionStage<Response> get() {
        AsyncSession session = driver.asyncSession();
        return session
                .runAsync("MATCH (f:Fruit) RETURN f ORDER BY f.name")
                .thenCompose(cursor ->
                        cursor.listAsync(record -> Fruit.from(record.get("f").asNode()))
                )
                .thenCompose(fruits ->
                        session.closeAsync().thenApply(signal -> fruits)
                )
                .thenApply(Response::ok)
                .thenApply(Response.ResponseBuilder::build);
    }

    @POST
    public CompletionStage<Response> create(Fruit fruit) {
        AsyncSession session = driver.asyncSession();
        return session
                .writeTransactionAsync(tx -> tx
                        .runAsync("CREATE (f:Fruit {name: $name}) RETURN f", Values.parameters("name", fruit.name))
                        .thenCompose(ResultCursor::singleAsync)
                )
                .thenApply(record -> Fruit.from(record.get("f").asNode()))
                .thenCompose(persistedFruit -> session.closeAsync().thenApply(signal -> persistedFruit))
                .thenApply(persistedFruit -> Response
                        .created(URI.create("/fruits/" + persistedFruit.id))
                        .build()
                );
    }

    @GET
    @Path("{id}")
    public CompletionStage<Response> getSingle(@PathParam("id") Long id) {
        AsyncSession session = driver.asyncSession();
        return session
                .readTransactionAsync(tx -> tx
                        .runAsync("MATCH (f:Fruit) WHERE id(f) = $id RETURN f", Values.parameters("id", id))
                        .thenCompose(ResultCursor::singleAsync)
                )
                .handle((record, exception) -> {
                    if(exception != null) {
                        Throwable source = exception;
                        if(exception instanceof CompletionException) {
                            source = exception.getCause();
                        }
                        Response.Status status = Response.Status.INTERNAL_SERVER_ERROR;
                        if(source instanceof NoSuchRecordException) {
                            status = Response.Status.NOT_FOUND;
                        }
                        return Response.status(status).build();
                    } else  {
                        return Response.ok(Fruit.from(record.get("f").asNode())).build();
                    }
                })
                .thenCompose(response -> session.closeAsync().thenApply(signal -> response));
    }

    @DELETE
    @Path("{id}")
    public CompletionStage<Response> delete(@PathParam("id") Long id) {

        AsyncSession session = driver.asyncSession();
        return session
                .writeTransactionAsync(tx -> tx
                        .runAsync("MATCH (f:Fruit) WHERE id(f) = $id DELETE f", Values.parameters("id", id))
                        .thenCompose(ResultCursor::consumeAsync)
                )
                .thenCompose(response -> session.closeAsync())
                .thenApply(signal -> Response.noContent().build());
    }
}