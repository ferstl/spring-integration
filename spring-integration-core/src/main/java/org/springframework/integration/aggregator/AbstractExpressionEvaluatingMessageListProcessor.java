/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.aggregator;

import java.util.Collection;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.expression.MapAccessor;
import org.springframework.core.convert.ConversionService;
import org.springframework.expression.AccessException;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParseException;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;
import org.springframework.integration.Message;
import org.springframework.integration.handler.ExpressionEvaluatingMessageProcessor;
import org.springframework.integration.transformer.MessageTransformationException;

/**
 * A base class for aggregators that evaluates a SpEL expression with the message list as the root object within the
 * evaluation context.
 * 
 * @author Dave Syer
 * @since 2.0
 */
public class AbstractExpressionEvaluatingMessageListProcessor implements BeanFactoryAware {

	private final ExpressionParser parser = new SpelExpressionParser(new SpelParserConfiguration(true, true));

	private final Expression expression;

	private volatile Class<?> expectedType = null;

	private final StandardEvaluationContext evaluationContext = new StandardEvaluationContext();

	/**
	 * Create an {@link ExpressionEvaluatingMessageProcessor} for the given expression String.
	 */
	public AbstractExpressionEvaluatingMessageListProcessor(String expression) {
		try {
			this.expression = parser.parseExpression(expression);
			this.getEvaluationContext().addPropertyAccessor(new MapAccessor());
		}
		catch (ParseException e) {
			throw new IllegalArgumentException("Failed to parse expression.", e);
		}
	}

	/**
	 * Set the result type expected from evaluation of the expression.
	 */
	public void setExpectedType(Class<?> expectedType) {
		this.expectedType = expectedType;
	}

	/**
	 * Specify a BeanFactory in order to enable resolution via <code>@beanName</code> in the expression.
	 */
	public void setBeanFactory(final BeanFactory beanFactory) {
		if (beanFactory != null) {
			this.getEvaluationContext().setBeanResolver(new BeanResolver() {
				public Object resolve(EvaluationContext context, String beanName) throws AccessException {
					return beanFactory.getBean(beanName);
				}
			});
		}
	}

	public void setConversionService(ConversionService conversionService) {
		if (conversionService != null) {
			this.evaluationContext.setTypeConverter(new StandardTypeConverter(conversionService));
		}
	}

	protected StandardEvaluationContext getEvaluationContext() {
		return this.evaluationContext;
	}

	protected Object evaluateExpression(Expression expression, Collection<? extends Message<?>> messages,
			Class<?> expectedType) {
		try {
			return (expectedType != null) ? expression.getValue(this.evaluationContext, messages, expectedType)
					: expression.getValue(this.evaluationContext, messages);
		}
		catch (EvaluationException e) {
			Throwable cause = e.getCause();
			throw new MessageTransformationException("Expression evaluation failed.", cause == null ? e : cause);
		}
		catch (Exception e) {
			throw new MessageTransformationException("Expression evaluation failed.", e);
		}
	}

	/**
	 * Processes the Message by evaluating the expression with that Message as the root object. The expression
	 * evaluation result Object will be returned.
	 */
	protected Object process(Collection<? extends Message<?>> messages) {
		return this.evaluateExpression(this.expression, messages, this.expectedType);
	}

}