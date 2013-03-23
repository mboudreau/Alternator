package com.michelboudreau.alternator;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodb.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodb.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodb.model.BatchGetItemResult;
import com.amazonaws.services.dynamodb.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodb.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.CreateTableResult;
import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemResult;
import com.amazonaws.services.dynamodb.model.DeleteTableRequest;
import com.amazonaws.services.dynamodb.model.DeleteTableResult;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableResult;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.ListTablesRequest;
import com.amazonaws.services.dynamodb.model.ListTablesResult;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.QueryRequest;
import com.amazonaws.services.dynamodb.model.QueryResult;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.amazonaws.services.dynamodb.model.UpdateItemResult;
import com.amazonaws.services.dynamodb.model.UpdateTableRequest;
import com.amazonaws.services.dynamodb.model.UpdateTableResult;

public class AlternatorDBAsyncClient extends AlternatorDBClient implements AmazonDynamoDBAsync {
	@Override
	public Future<ListTablesResult> listTablesAsync(ListTablesRequest listTablesRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<ListTablesResult>(listTables(listTablesRequest));
	}

	@Override
	public Future<QueryResult> queryAsync(QueryRequest queryRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<QueryResult>(query(queryRequest));
	}

	@Override
	public Future<BatchWriteItemResult> batchWriteItemAsync(BatchWriteItemRequest batchWriteItemRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<BatchWriteItemResult>(batchWriteItem(batchWriteItemRequest));
	}

	@Override
	public Future<UpdateItemResult> updateItemAsync(UpdateItemRequest updateItemRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<UpdateItemResult>(updateItem(updateItemRequest));
	}

	@Override
	public Future<PutItemResult> putItemAsync(PutItemRequest putItemRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<PutItemResult>(putItem(putItemRequest));
	}

	@Override
	public Future<DescribeTableResult> describeTableAsync(DescribeTableRequest describeTableRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<DescribeTableResult>(describeTable(describeTableRequest));
	}

	@Override
	public Future<ScanResult> scanAsync(ScanRequest scanRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<ScanResult>(scan(scanRequest));
	}

	@Override
	public Future<CreateTableResult> createTableAsync(CreateTableRequest createTableRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<CreateTableResult>(createTable(createTableRequest));
	}

	@Override
	public Future<UpdateTableResult> updateTableAsync(UpdateTableRequest updateTableRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<UpdateTableResult>(updateTable(updateTableRequest));
	}

	@Override
	public Future<DeleteTableResult> deleteTableAsync(DeleteTableRequest deleteTableRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<DeleteTableResult>(deleteTable(deleteTableRequest));
	}

	@Override
	public Future<DeleteItemResult> deleteItemAsync(DeleteItemRequest deleteItemRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<DeleteItemResult>(deleteItem(deleteItemRequest));
	}

	@Override
	public Future<GetItemResult> getItemAsync(GetItemRequest getItemRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<GetItemResult>(getItem(getItemRequest));
	}

	@Override
	public Future<BatchGetItemResult> batchGetItemAsync(BatchGetItemRequest batchGetItemRequest) throws AmazonServiceException, AmazonClientException {
		return new FakeFuture<BatchGetItemResult>(batchGetItem(batchGetItemRequest));
	}
	
	class FakeFuture<T> implements Future<T> {
		private final T value;

		public FakeFuture(final T value) {
			this.value = value;
		}

		@Override
		public boolean cancel(final boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			return value;
		}

		@Override
		public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return value;
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return true;
		}
	}

	@Override
	public Future<ListTablesResult> listTablesAsync(ListTablesRequest listTablesRequest, AsyncHandler<ListTablesRequest, ListTablesResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<QueryResult> queryAsync(QueryRequest queryRequest, AsyncHandler<QueryRequest, QueryResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<BatchWriteItemResult> batchWriteItemAsync(BatchWriteItemRequest batchWriteItemRequest, AsyncHandler<BatchWriteItemRequest, BatchWriteItemResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<UpdateItemResult> updateItemAsync(UpdateItemRequest updateItemRequest, AsyncHandler<UpdateItemRequest, UpdateItemResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<PutItemResult> putItemAsync(PutItemRequest putItemRequest, AsyncHandler<PutItemRequest, PutItemResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<DescribeTableResult> describeTableAsync(DescribeTableRequest describeTableRequest, AsyncHandler<DescribeTableRequest, DescribeTableResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<ScanResult> scanAsync(ScanRequest scanRequest, AsyncHandler<ScanRequest, ScanResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<CreateTableResult> createTableAsync(CreateTableRequest createTableRequest, AsyncHandler<CreateTableRequest, CreateTableResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<UpdateTableResult> updateTableAsync(UpdateTableRequest updateTableRequest, AsyncHandler<UpdateTableRequest, UpdateTableResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<DeleteTableResult> deleteTableAsync(DeleteTableRequest deleteTableRequest, AsyncHandler<DeleteTableRequest, DeleteTableResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<DeleteItemResult> deleteItemAsync(DeleteItemRequest deleteItemRequest, AsyncHandler<DeleteItemRequest, DeleteItemResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<GetItemResult> getItemAsync(GetItemRequest getItemRequest, AsyncHandler<GetItemRequest, GetItemResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Future<BatchGetItemResult> batchGetItemAsync(BatchGetItemRequest batchGetItemRequest, AsyncHandler<BatchGetItemRequest, BatchGetItemResult> asyncHandler) throws AmazonServiceException, AmazonClientException {
		throw new UnsupportedOperationException();
	}
}
