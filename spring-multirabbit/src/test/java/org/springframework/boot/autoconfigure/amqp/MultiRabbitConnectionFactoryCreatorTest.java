package org.springframework.boot.autoconfigure.amqp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import com.rabbitmq.client.impl.CredentialsProvider;
import com.rabbitmq.client.impl.CredentialsRefreshService;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.rabbit.connection.RoutingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleResourceHolder;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ResourceLoader;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MultiRabbitConnectionFactoryCreatorTest {

    private static final String DUMMY_KEY = "dummy-key";

    @Mock
    private ConfigurableListableBeanFactory beanFactory;

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private ObjectProvider<ConnectionNameStrategy> connectionNameStrategy;

    @Mock
    private MultiRabbitConnectionFactoryWrapper wrapper;

    @Mock
    private ConnectionFactory connectionFactory0;

    @Mock
    private ConnectionFactory connectionFactory1;

    @Mock
    private SimpleRabbitListenerContainerFactory containerFactory;

    @Mock
    private RabbitAdmin rabbitAdmin;

    @Mock
    private RabbitProperties rabbitProperties;

    @Mock
    private RabbitProperties secondaryRabbitProperties;

    @Mock
    private MultiRabbitProperties multiRabbitProperties;

    @Mock
    private RabbitAutoConfiguration.RabbitConnectionFactoryCreator springFactoryCreator;

    @Mock
    private ResourceLoader resourceLoader;

    @Mock
    private ObjectProvider<CredentialsProvider> credentialsProvider;

    @Mock
    private ObjectProvider<CredentialsRefreshService> credentialsRefreshService;

    private MultiRabbitAutoConfiguration.MultiRabbitConnectionFactoryCreator creator() {
        MultiRabbitAutoConfiguration.MultiRabbitConnectionFactoryCreator config
            = new MultiRabbitAutoConfiguration.MultiRabbitConnectionFactoryCreator(
            springFactoryCreator, connectionNameStrategy, resourceLoader, credentialsProvider,
            credentialsRefreshService);
        config.setBeanFactory(beanFactory);
        config.setApplicationContext(applicationContext);
        return config;
    }

    @Test
    void shouldInstantiateExternalEmptyWrapper() {
        MultiRabbitConnectionFactoryWrapper emptyWrapper = creator().externalEmptyWrapper();
        assertTrue(emptyWrapper.getConnectionFactories().isEmpty());
        assertNull(emptyWrapper.getDefaultConnectionFactory());
    }

    @Test
    void shouldInstantiateRoutingConnectionFactory() throws Exception {
        when(springFactoryCreator.rabbitConnectionFactory(rabbitProperties, resourceLoader, credentialsProvider,
            credentialsRefreshService, connectionNameStrategy)
        ).thenReturn(new CachingConnectionFactory());

        assertTrue(creator().routingConnectionFactory(rabbitProperties, multiRabbitProperties, wrapper)
                instanceof RoutingConnectionFactory);
    }

    @Test
    void shouldInstantiateRoutingConnectionFactoryWithDefaultAndMultipleConnections() throws Exception {
        when(wrapper.getDefaultConnectionFactory()).thenReturn(connectionFactory0);
        when(wrapper.getConnectionFactories()).thenReturn(singletonMap(DUMMY_KEY, connectionFactory1));
        when(wrapper.getContainerFactories()).thenReturn(singletonMap(DUMMY_KEY, containerFactory));
        when(wrapper.getRabbitAdmins()).thenReturn(singletonMap(DUMMY_KEY, rabbitAdmin));

        ConnectionFactory routingConnectionFactory = creator().routingConnectionFactory(rabbitProperties,
                multiRabbitProperties, wrapper);

        assertTrue(routingConnectionFactory instanceof SimpleRoutingConnectionFactory);
        verify(beanFactory).registerSingleton(DUMMY_KEY, containerFactory);
        verify(beanFactory).registerSingleton(DUMMY_KEY + MultiRabbitConstants.RABBIT_ADMIN_SUFFIX,
                rabbitAdmin);
        verifyNoMoreInteractions(beanFactory);
    }

    @Test
    void shouldInstantiateRoutingConnectionFactoryWithOnlyDefaultConnectionFactory() throws Exception {
        when(wrapper.getDefaultConnectionFactory()).thenReturn(connectionFactory0);

        ConnectionFactory routingConnectionFactory = creator().routingConnectionFactory(rabbitProperties,
                multiRabbitProperties, wrapper);

        assertTrue(routingConnectionFactory instanceof SimpleRoutingConnectionFactory);
        verifyNoMoreInteractions(beanFactory);
    }

    @Test
    void shouldInstantiateRoutingConnectionFactoryWithOnlyMultipleConnectionFactories() throws Exception {
        when(wrapper.getConnectionFactories()).thenReturn(singletonMap(DUMMY_KEY, connectionFactory0));
        when(wrapper.getContainerFactories()).thenReturn(singletonMap(DUMMY_KEY, containerFactory));
        when(wrapper.getRabbitAdmins()).thenReturn(singletonMap(DUMMY_KEY, rabbitAdmin));
        when(springFactoryCreator.rabbitConnectionFactory(rabbitProperties, resourceLoader, credentialsProvider,
            credentialsRefreshService, connectionNameStrategy)
        ).thenReturn(new CachingConnectionFactory());

        ConnectionFactory routingConnectionFactory = creator().routingConnectionFactory(rabbitProperties,
                multiRabbitProperties, wrapper);

        assertTrue(routingConnectionFactory instanceof SimpleRoutingConnectionFactory);
        verify(beanFactory).registerSingleton(DUMMY_KEY, containerFactory);
        verify(beanFactory).registerSingleton(DUMMY_KEY + MultiRabbitConstants.RABBIT_ADMIN_SUFFIX,
                rabbitAdmin);
        verifyNoMoreInteractions(beanFactory);
    }

    @Test
    void shouldReachDefaultConnectionFactoryWhenNotBound() throws Exception {
        when(wrapper.getDefaultConnectionFactory()).thenReturn(connectionFactory0);
        when(wrapper.getConnectionFactories()).thenReturn(singletonMap(DUMMY_KEY, connectionFactory1));
        when(wrapper.getContainerFactories()).thenReturn(singletonMap(DUMMY_KEY, containerFactory));
        when(wrapper.getRabbitAdmins()).thenReturn(singletonMap(DUMMY_KEY, rabbitAdmin));

        creator().routingConnectionFactory(rabbitProperties, multiRabbitProperties, wrapper).getVirtualHost();

        verify(connectionFactory0).getVirtualHost();
        verify(connectionFactory1, never()).getVirtualHost();
    }

    @Test
    void shouldBindAndReachMultiConnectionFactory() throws Exception {
        when(wrapper.getDefaultConnectionFactory()).thenReturn(connectionFactory0);
        when(wrapper.getConnectionFactories()).thenReturn(singletonMap(DUMMY_KEY, connectionFactory1));
        when(wrapper.getContainerFactories()).thenReturn(singletonMap(DUMMY_KEY, containerFactory));
        when(wrapper.getRabbitAdmins()).thenReturn(singletonMap(DUMMY_KEY, rabbitAdmin));

        ConnectionFactory routingConnectionFactory = creator().routingConnectionFactory(rabbitProperties,
                multiRabbitProperties, wrapper);

        SimpleResourceHolder.bind(routingConnectionFactory, DUMMY_KEY);
        routingConnectionFactory.getVirtualHost();
        SimpleResourceHolder.unbind(routingConnectionFactory);

        verify(connectionFactory0, never()).getVirtualHost();
        verify(connectionFactory1).getVirtualHost();
    }

    @Test
    void shouldInstantiateMultiRabbitConnectionFactoryWrapperWithDefaultConnection() throws Exception {
        when(springFactoryCreator.rabbitConnectionFactory(rabbitProperties, resourceLoader, credentialsProvider,
            credentialsRefreshService, connectionNameStrategy)
        ).thenReturn(new CachingConnectionFactory());

        assertNotNull(creator().routingConnectionFactory(rabbitProperties, null, wrapper));
    }

    @Test
    void shouldInstantiateMultiRabbitConnectionFactoryWrapperWithMultipleConnections() throws Exception {
        when(springFactoryCreator.rabbitConnectionFactory(
            any(RabbitProperties.class),
            eq(resourceLoader),
            eq(credentialsProvider),
            eq(credentialsRefreshService),
            eq(connectionNameStrategy))
        ).thenReturn(new CachingConnectionFactory());

        MultiRabbitProperties multiRabbitProperties = new MultiRabbitProperties();
        multiRabbitProperties.getConnections().put(DUMMY_KEY, secondaryRabbitProperties);
        multiRabbitProperties.setDefaultConnection(DUMMY_KEY);

        creator().routingConnectionFactory(null, multiRabbitProperties, wrapper);

        verify(springFactoryCreator).rabbitConnectionFactory(secondaryRabbitProperties, resourceLoader,
            credentialsProvider, credentialsRefreshService, connectionNameStrategy);
    }

    @Test
    void shouldInstantiateMultiRabbitConnectionFactoryWrapperWithDefaultAndMultipleConnections()
            throws Exception {
        when(springFactoryCreator.rabbitConnectionFactory(
            any(RabbitProperties.class),
            eq(resourceLoader),
            eq(credentialsProvider),
            eq(credentialsRefreshService),
            eq(connectionNameStrategy))
        ).thenReturn(new CachingConnectionFactory());

        MultiRabbitProperties multiRabbitProperties = new MultiRabbitProperties();
        multiRabbitProperties.getConnections().put(DUMMY_KEY, secondaryRabbitProperties);

        creator().routingConnectionFactory(rabbitProperties, multiRabbitProperties, wrapper);

        verify(springFactoryCreator).rabbitConnectionFactory(rabbitProperties, resourceLoader, credentialsProvider,
            credentialsRefreshService, connectionNameStrategy);
        verify(springFactoryCreator).rabbitConnectionFactory(secondaryRabbitProperties, resourceLoader,
            credentialsProvider, credentialsRefreshService, connectionNameStrategy);
    }

    @Test
    void shouldEncapsulateExceptionWhenFailingToCreateBean() throws Exception {
        when(springFactoryCreator.rabbitConnectionFactory(
            any(RabbitProperties.class),
            eq(resourceLoader),
            eq(credentialsProvider),
            eq(credentialsRefreshService),
            eq(connectionNameStrategy))
        ).thenThrow(new Exception("mocked-exception"));

        MultiRabbitProperties multiRabbitProperties = new MultiRabbitProperties();
        multiRabbitProperties.getConnections().put(DUMMY_KEY, secondaryRabbitProperties);

        final Executable executable = () -> creator().routingConnectionFactory(rabbitProperties, multiRabbitProperties,
                wrapper);

        assertThrows(Exception.class, executable, "mocked-exception");
    }
}
