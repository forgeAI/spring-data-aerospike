package org.springframework.data.aerospike.repository.support;

import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public class SimpleAerospikeRepository<T, ID extends Serializable> implements AerospikeRepository<T, ID> {

	private final AerospikeOperations operations;
	private final EntityInformation<T, ID> entityInformation;

	
	public SimpleAerospikeRepository(EntityInformation<T, ID> metadata,
			AerospikeOperations operations) {
		this.entityInformation = metadata;
		this.operations = operations;
	}
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findOne(java.io.Serializable)
	 */
	@Override
	public Optional<T> findById(ID id) {
		return Optional.ofNullable(operations.findById(id, entityInformation.getJavaType()));
	}

	@Override
	public <S extends T> S save(S entity) {
		operations.save(entity);
		return entity;
	}

	@Override
	public <S extends T> List<S> saveAll(Iterable<S> entities) {
		Assert.notNull(entities, "The given Iterable of entities not be null!");

		List<S> result = IterableConverter.toList(entities);
		for (S entity : result) {
			save(entity);
		}

		return result;
	}
	
	@Override
	public void delete(T entity) {
		operations.delete(entity);
	}



	/* (non-Javadoc)
	 * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Sort)
	 */
	@Override
	public Iterable<T> findAll(Sort sort) {
		return operations.findAll(sort, entityInformation.getJavaType());
	}
	/* (non-Javadoc)
	 * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Pageable)
	 */

	@Override
	public Page<T> findAll(Pageable pageable) {

		if (pageable == null) {
			List<T> result = findAll();
			return new PageImpl<T>(result, null, result.size());
		}

		// TODO: Aerospike findInRange assumes int for offset.  Check for lossy data here.
		Assert.state(pageable.getOffset() < Integer.MAX_VALUE);
		Iterable<T> content = operations.findInRange((int)pageable.getOffset(), pageable.getPageSize(), pageable.getSort(),entityInformation.getJavaType());

		String setName = operations.getSetName(entityInformation.getJavaType());
		
		long count = this.operations.count(entityInformation.getJavaType(), setName);
		
		return new PageImpl<T>(IterableConverter.toList(content), pageable, count);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#exists(java.io.Serializable)
	 */
	@Override
	public boolean existsById(ID id) {
		return operations.exists(id, entityInformation.getJavaType());
	}
	/* (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll()
	 */

	@Override
	public List<T> findAll() {
		return IterableConverter.toList(operations.findAll(entityInformation.getJavaType()));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll(java.lang.Iterable)
	 */
	@Override
	public Iterable<T> findAllById(Iterable<ID> ids) {
		Assert.notNull(ids, "List of ids must not be null!");

		List<ID> idList = IterableConverter.toList(ids);
		return operations.findByIds(idList, entityInformation.getJavaType());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#count()
	 */
	@Override
	public long count() {
		return operations.count(entityInformation.getJavaType());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.io.Serializable)
	 */
	@Override
	public void deleteById(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		operations.delete(id, entityInformation.getJavaType());
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Iterable)
	 */
	@Override
	public void deleteAll(Iterable<? extends T> entities) {
		for (T entity : entities) {
			delete(entity);
		}
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#deleteAll()
	 */
	@Override
	public void deleteAll() {
		operations.delete(entityInformation.getJavaType());
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.repository.AerospikeRepository#createIndex(java.lang.Class, java.lang.String, java.lang.String, com.aerospike.client.query.IndexType)
	 */
	@Override
	public <T> void createIndex(Class<T> domainType, String indexName, String binName, IndexType indexType) {
		operations.createIndex(domainType, indexName, binName, indexType);
	}

	@Override
	public <T> void deleteIndex(Class<T> domainType, String indexName) {
		operations.deleteIndex(domainType, indexName);
	}

	@Override
	public boolean indexExists(String indexName) {
		return operations.indexExists(indexName);
	}
}
