package org.springframework.data.aerospike.repository;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.sample.CompositeObject;
import org.springframework.data.aerospike.sample.CompositeObjectRepository;
import org.springframework.data.aerospike.sample.SimpleObject;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

public class RepositoriesIntegrationTests extends BaseIntegrationTests {

    @Autowired
    CompositeObjectRepository repository;

    @Test
    public void findById_shouldReturnNullForNonExistingKey() throws Exception {
        Optional<CompositeObject> one = repository.findById("non-existing-id");

        assertThat(one).isNotNull();
        assertThat(one.isPresent()).isFalse();
    }

    @Test
    public void shouldSaveObjectWithPersistenceConstructorThatHasAllFields() throws Exception {
        CompositeObject expected = CompositeObject.builder()
                .id("composite-object-1")
                .intValue(15)
                .simpleObject(SimpleObject.builder().property1("prop1").property2(555).build())
                .build();
        repository.save(expected);

        Optional<CompositeObject> actual = repository.findById(expected.getId());

        assertThat(actual.get()).isEqualTo(expected);
    }

    @Test
    public void shouldDeleteObjectWithPersistenceConstructor() throws Exception {
        String id = nextId();
        CompositeObject expected = CompositeObject.builder()
                .id(id)
                .build();
        repository.save(expected);
        assertThat(repository.findById(id).isPresent()).isTrue();

        repository.delete(expected);

        assertThat(repository.findById(id).isPresent()).isFalse();
    }
}
