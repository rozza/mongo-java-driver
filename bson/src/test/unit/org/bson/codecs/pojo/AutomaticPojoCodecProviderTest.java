/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo;

import org.bson.Document;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.pojo.entities.CollectionNestedPojoModel;
import org.bson.codecs.pojo.entities.ConstructorNotPublicModel;
import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.bson.codecs.pojo.entities.MultipleBoundsModel;
import org.bson.codecs.pojo.entities.MultipleLevelGenericModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderMapModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderModel;
import org.bson.codecs.pojo.entities.NestedGenericTreeModel;
import org.bson.codecs.pojo.entities.NestedMultipleLevelGenericModel;
import org.bson.codecs.pojo.entities.ShapeHolderModel;
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.bson.codecs.pojo.entities.UpperBoundsConcreteModel;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorModel;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

public final class AutomaticPojoCodecProviderTest extends PojoTestCase {

    @Test
    public void testRoundTripSimpleModel() {
        SimpleModel model = getSimpleModel();
        roundTrip(model, SIMPLE_MODEL_JSON);
    }

    @Test
    public void testRoundTripCollectionNestedPojoModel() {
        CollectionNestedPojoModel model = getCollectionNestedPojoModel();
        roundTrip(model,
                "{ 'listSimple': [" + SIMPLE_MODEL_JSON + "],"
                        + "'listListSimple': [[" + SIMPLE_MODEL_JSON + "]],"
                        + "'setSimple': [" + SIMPLE_MODEL_JSON + "],"
                        + "'setSetSimple': [[" + SIMPLE_MODEL_JSON + "]],"
                        + "'mapSimple': {'s': " + SIMPLE_MODEL_JSON + "},"
                        + "'mapMapSimple': {'ms': {'s': " + SIMPLE_MODEL_JSON + "}},"
                        + "'mapListSimple': {'ls': [" + SIMPLE_MODEL_JSON + "]},"
                        + "'mapListMapSimple': {'lm': [{'s': " + SIMPLE_MODEL_JSON + "}]},"
                        + "'mapSetSimple': {'s': [" + SIMPLE_MODEL_JSON + "]},"
                        + "'listMapSimple': [{'s': " + SIMPLE_MODEL_JSON + "}],"
                        + "'listMapListSimple': [{'ls': [" + SIMPLE_MODEL_JSON + "]}],"
                        + "'listMapSetSimple': [{'s': [" + SIMPLE_MODEL_JSON + "]}],"
                        + "}");
    }

    @Test
    public void testShapeModelAbstract() {
        roundTrip(new ShapeHolderModel(getShapeModelCircle()),
                "{'shape': {'_t': 'ShapeModelCircle', 'color': 'orange', 'radius': 4.2}}");

        roundTrip(new ShapeHolderModel(getShapeModelRectangle()),
                "{'shape': {'_t': 'ShapeModelRectangle', 'color': 'green', 'width': 22.1, 'height': 105.0} }");
    }

    @Test
    public void testInheritedDiscriminatorAnnotation() {
        roundTrip(getShapeModelCircle(),
                "{'_t': 'ShapeModelCircle', 'color': 'orange', 'radius': 4.2}");

        roundTrip(getShapeModelRectangle(),
                "{'_t': 'ShapeModelRectangle', 'color': 'green', 'width': 22.1, 'height': 105.0}");
    }

    @Test
    public void testUpperBoundsConcreteModel() {
        roundTrip(new UpperBoundsConcreteModel(1L),
                "{'myGenericField': {'$numberLong': '1'}}");
    }

    @Test
    public void testNestedGenericHolderModel() {
        PojoCodecProvider.Builder builder = getPojoCodecProviderBuilder(NestedGenericHolderModel.class, GenericHolderModel.class);
        roundTrip(builder, getNestedGenericHolderModel(),
                "{'nested': {'myGenericField': 'generic', 'myLongField': {'$numberLong': '1'}}}");
    }

    @Test
    public void testNestedGenericHolderMapModel() {
        PojoCodecProvider.Builder builder = getPojoCodecProviderBuilder(NestedGenericHolderMapModel.class,
                GenericHolderModel.class, SimpleGenericsModel.class, SimpleModel.class);
        roundTrip(builder, getNestedGenericHolderMapModel(),
                "{ 'nested': { 'myGenericField': {'s': " + SIMPLE_MODEL_JSON + "}, 'myLongField': {'$numberLong': '1'}}}");
    }

    @Test
    public void testNestedReusedGenericsModel() {
        roundTrip(getNestedReusedGenericsModel(),
                "{ 'nested':{ 'field1':{ '$numberLong':'1' }, 'field2':[" + SIMPLE_MODEL_JSON + "], "
                        + "'field3':'field3', 'field4':42, 'field5':'field5', 'field6':[" + SIMPLE_MODEL_JSON + ", "
                        + SIMPLE_MODEL_JSON + "], 'field7':{ '$numberLong':'2' }, 'field8':'field8' } }");
    }

    @Test
    public void testMultipleBoundsModel() {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("key", "value");
        List<Integer> list = asList(1, 2, 3);
        roundTrip(new MultipleBoundsModel(map, list, 2.2),
                "{'level1' : 2.2, 'level2': [1, 2, 3], 'level3': {key: 'value'}}");
    }

    @Test
    public void testNestedGenericTreeModel(){
        roundTrip(new NestedGenericTreeModel(42, getGenericTreeModel()),
                "{'intField': 42, 'nested': {'field1': 'top', 'field2': 1, "
                        + "'left': {'field1': 'left', 'field2': 2, 'left': {'field1': 'left', 'field2': 3}}, "
                        + "'right': {'field1': 'right', 'field2': 4, 'left': {'field1': 'left', 'field2': 5}}}}");
    }

    @Test
    public void testNestedMultipleLevelGenericModel() {
        String json = "{'intField': 42, 'nested': {'stringField': 'string', 'nested': {'field1': 'top', 'field2': 1, "
                + "'left': {'field1': 'left', 'field2': 2, 'left': {'field1': 'left', 'field2': 3}}, "
                + "'right': {'field1': 'right', 'field2': 4, 'left': {'field1': 'left', 'field2': 5}}}}}";

        roundTrip(new NestedMultipleLevelGenericModel(42, new MultipleLevelGenericModel<String>("string", getGenericTreeModel())),
                json);
    }

    @Test
    public void testCreatorConstructorModel() {
        CreatorConstructorModel model = new CreatorConstructorModel(10, "eleven", 12);
        roundTrip(model,
                "{'integerField': 10, 'stringField': 'eleven', 'longField': {$numberLong: '12'}}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testNoProperties() {
        encodesTo(fromProviders(new AutomaticPojoCodecProvider()), 101, "");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testNoConstructor() {
        roundTrip(ConstructorNotPublicModel.create(10), "{integerField: 10}");
    }

    @Test (expected = CodecConfigurationException.class)
    public void testUnknownMapGenericTypes() {
        roundTrip(new HashMap<String, String>(), "{}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testUnknownCollectionGenericTypes() {
        roundTrip(new ArrayList<String>(), "{}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidMapImplementation() {
        roundTrip(new Document(), "{}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testUnspecializedModel() {
        roundTrip(new GenericHolderModel<String>(), "{}");
    }

}
