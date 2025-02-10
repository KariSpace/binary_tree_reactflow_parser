from collections import deque
import json
from bson.objectid import ObjectId
from dev_database.utils.product_utils import get_product_info_by_shop_data, get_stock_by_sku_and_pharmacy
from dev_database.utils.pharmacy_utils import PharmacyHandler



class FlowProcessor:
    def __init__(self, nodes, edges, session_manager, shopify_manager):
        """
        Initialize the FlowProcessor class.

        :param nodes: List of nodes in the flow.
        :param edges: List of edges connecting the nodes.
        :param session_manager: Manager for handling session-related operations.
        :param shopify_manager: Manager for handling Shopify-related operations.
        """
        self.__nodes = nodes
        self.__edges = edges
        self.result = []  
        self.session_manager = session_manager
        self.shopify_manager = shopify_manager

    def get_node_by_id(self, node_id):
        """
        Retrieve a node by its identifier.

        :param node_id: The identifier of the node to retrieve.
        :return: The node if found, otherwise None.
        """
        return next((node for node in self.__nodes if node['id'] == node_id), None)

    def get_edges_by_source(self, source_id):
        """
        Retrieve all edges originating from a given source.

        :param source_id: The identifier of the source node.
        :return: List of edges originating from the source.
        """
        return [edge for edge in self.__edges if edge['source'] == source_id]

    def __process_condition(self, condition, order):
        """
        Process a condition for the given order.

        :param condition: The condition to evaluate.
        :param order: The order data to evaluate against the condition.
        :return: True if the condition is met, otherwise False.
        """
        try:
            field, operator, value = condition.split('|')  # Split the condition into parts
            value = json.loads(value)  # Decode the value from JSON

            # Define a class for evaluating conditions
            class ConditionEvaluator:
                def __init__(self, order):
                    """
                    Initialize the ConditionEvaluator with the order.

                    :param order: The order data to evaluate.
                    """
                    self.order = order  # Store the order for evaluation

                def evaluate_tag(self, operator, value):
                    """
                    Evaluate tag conditions.

                    :param operator: The operator to use for evaluation.
                    :param value: The value to check against.
                    :return: True if the condition is met, otherwise False.
                    """
                    if operator == 'contains':
                        return value['name'] in self.order['tags']
                    elif operator == 'not contains':
                        return not (value['name'] in self.order['tags'])

                def evaluate_price(self, operator, value):
                    """
                    Evaluate price conditions.

                    :param operator: The operator to use for evaluation.
                    :param value: The value to check against.
                    :return: Result of the price evaluation.
                    """
                    return eval(f"{self.order['price']} {operator} {value['name']}")

                def evaluate_stock(self, operator, value):
                    """
                    Evaluate stock conditions.

                    :param operator: The operator to use for evaluation.
                    :param value: The value to check against.
                    :return: Result of the stock evaluation.
                    """
                    current = get_stock_by_product_location(self.session_manager, self.shopify_manager,
                                                            value.get("variant", {}).get("id", None),
                                                            value.get("pharmacy", {}).get("id", None))
                    expected_stock = json.loads(value.get('value')).get('id')
                    return eval(f"{int(current)} {operator} {int(expected_stock)}")

                def evaluate_line_items(self, operator, value):
                    """
                    Evaluate conditions based on line items.

                    :param operator: The operator to use for evaluation.
                    :param value: The value to check against.
                    :return: Result of the line items evaluation.
                    """
                    # Define methods for processing line item types
                    line_item_methods = {
                        "collection": self._evaluate_collection,
                        "product_variant": self._evaluate_product_variant,
                        "product": self._evaluate_product
                    }

                    # Call the method based on the type
                    if value['type'] in line_item_methods:
                        return line_item_methods[value['type']](operator, value)

                def _evaluate_collection(self, operator, value):
                    """
                    Evaluate collection conditions.

                    :param operator: The operator to use for evaluation.
                    :param value: The value to check against.
                    :return: Result of the collection evaluation.
                    """
                    collection_items = self.shopify_manager.get_collection_by_id(value['id'])
                    collection_item_ids = [edge['node']['id'].replace('gid://shopify/Product/', '') for edge in
                                            collection_items['data']['collection']['products']['edges']]
                    return self._evaluate_items(operator, collection_item_ids, 'product_id')

                def _evaluate_product_variant(self, operator, value):
                    """
                    Evaluate product variant conditions.

                    :param operator: The operator to use for evaluation.
                    :param value: The value to check against.
                    :return: Result of the product variant evaluation.
                    """
                    return self._evaluate_items(operator, [int(value['variant_id'])], 'variant_id')

                def _evaluate_product(self, operator, value):
                    """
                    Evaluate product conditions.

                    :param operator: The operator to use for evaluation.
                    :param value: The value to check against.
                    :return: Result of the product evaluation.
                    """
                    return self._evaluate_items(operator, [int(value['id'])], 'product_id')

                def _evaluate_items(self, operator, valid_ids, id_key):
                    """
                    Evaluate items based on the operator.

                    :param operator: The operator to use for evaluation.
                    :param valid_ids: List of valid IDs to check against.
                    :param id_key: The key to use for accessing IDs in line items.
                    :return: Result of the items evaluation.
                    """
                    if operator == 'contains':
                        return any(str(line_item[id_key]) in valid_ids for line_item in self.order['line_items'])
                    elif operator == 'not contains':
                        return not any(str(line_item[id_key]) in valid_ids for line_item in self.order['line_items'])
                    elif operator == 'full match':
                        return all(str(line_item[id_key]) in valid_ids for line_item in self.order['line_items'])

                def evaluate_length(self, operator, value):
                    """
                    Evaluate length conditions.

                    :param operator: The operator to use for evaluation.
                    :param value: The value to check against.
                    :return: Result of the length evaluation.
                    """
                    return eval(f"{len(self.order['line_items'])} {operator} {value['value']}")

            evaluator = ConditionEvaluator(order)  # Create an evaluator instance

            evaluation_methods = {
                'tag': evaluator.evaluate_tag,
                'price': evaluator.evaluate_price,
                'stock': evaluator.evaluate_stock,
                'line_items': evaluator.evaluate_line_items,
                'line_items-length': evaluator.evaluate_length
            }

            # Call the method based on the field
            if field in evaluation_methods:
                return evaluation_methods[field](operator, value)
        except Exception as e:
            print(f"Error evaluating condition: {condition}, error: {e}")
            return False

    def __process_split(self, node, order):
        """
        Process a split node in the flow.

        :param node: The split node to process.
        :param order: The order data to evaluate against the split condition.
        :return: Tuple of true and false orders based on the split condition.
        """
        condition = "line_items" + node['data']['label']
        true_items = []
        for item in order['line_items']:
            if self.__process_condition(condition, {'line_items': [item]}):
                true_items.append(item)

        true_items = [item for item in order['line_items'] if self.__process_condition(condition, {'line_items': [item]})]
        false_items = [item for item in order['line_items'] if item not in true_items]

        true_order = {'line_items': true_items}
        false_order = {'line_items': false_items}

        return true_order, false_order

    def __process_node(self, start_node, order):
        """
        Process a node in the flow.

        :param start_node: The starting node to process.
        :param order: The order data to evaluate against the node.
        """
        queue = deque([(start_node, order)])  # Initialize a queue for processing nodes

        while queue:
            node, current_order = queue.popleft()

            if node['type'] == 'start':
                next_edge = self.get_edges_by_source(node['id'])[0]
                next_node = self.get_node_by_id(next_edge['target'])
                queue.append((next_node, current_order))

            elif node['type'] == 'split':
                true_order, false_order = self.__process_split(node, current_order)
                start_edges = self.get_edges_by_source(node['id'])

                if true_order['line_items']:
                    true_edge = next(edge for edge in start_edges if edge['label'] == 'YES')
                    queue.append((self.get_node_by_id(true_edge['target']), true_order))
                if false_order['line_items']:
                    false_edge = next(edge for edge in start_edges if edge['label'] == 'NO')
                    queue.append((self.get_node_by_id(false_edge['target']), false_order))

            elif node['type'] == 'condition':
                condition = node['data']['label']
                next_node_id = None
                edges = self.get_edges_by_source(node['id'])

                if self.__process_condition(condition, current_order):
                    next_node_id = next(edge for edge in edges if edge['label'] == 'YES')['target']
                else:
                    next_node_id = next(edge for edge in edges if edge['label'] == 'NO')['target']

                queue.append((self.get_node_by_id(next_node_id), current_order))

            elif node['type'] == 'fullfill':
                label, fulfillment_option, extra_data = node['data']['label'].split('|')
                self.result.append({
                    "line_items": current_order['line_items'],
                    "pharmacy": json.loads(label),
                    "fulfillment_option": fulfillment_option,
                    "extra_data": json.loads(extra_data)
                })

    def process_order(self, order):
        """
        Process the given order through the flow.

        :param order: The order data to process.
        """
        start_node = next(n for n in self.__nodes if n['type'] == 'start')  # Find the starting node
        self.__process_node(start_node, order)  # Process the starting node

    def run(self, order):
        """
        Run the flow processor on the given order.

        :param order: The order data to process.
        :return: The result of the processing.
        """
        self.process_order(order) 
        return self.result


def check_fulfillment(mongo_client, order_data, session_manager, shopify_manager):
    """
    Check fulfillment for the given order data.

    :param mongo_client: MongoDB client for database operations.
    :param order_data: The order data to check fulfillment for.
    :param session_manager: Manager for handling session-related operations.
    :param shopify_manager: Manager for handling Shopify-related operations.
    :return: List of fulfillment results.
    """
    deployed_collection = mongo_client["ShopFlowDeployed"]
    flow_collection = mongo_client["Flow"]
    shop = shopify_manager.shop_identifier
    try:
        deployed_flow_id = deployed_collection.find({"shop": shop}).sort({'created_at': -1}).limit(1).next().get('flow_id')
        current_flow = flow_collection.find_one({"_id": ObjectId(deployed_flow_id)})
    except StopIteration:
        return []  # No flow to process the order, just continue in a good old way

    processor = FlowProcessor(current_flow.get("nodes", []), current_flow.get("edges", []), session_manager, shopify_manager)
    result = processor.run(order_data)  # Run the processor on the order data
    return result  


def get_stock_by_product_location(session_manager, shopify_manager, variant_id, location_id):
    """
    Get stock information for a product at a specific location.

    :param session_manager: Manager for handling session-related operations.
    :param shopify_manager: Manager for handling Shopify-related operations.
    :param variant_id: The ID of the product variant.
    :param location_id: The ID of the location to check stock for.
    :return: The stock information for the product at the specified location.
    """
    product = get_product_info_by_shop_data(session_manager, shopify_manager.shop_identifier, variant_id)
    pharmacy = PharmacyHandler.for_external_id_and_shop(session_manager, location_id, shopify_manager.shop.uid())
    stock = get_stock_by_sku_and_pharmacy(session_manager, product.get("sku").get("uid"), pharmacy.uid())
    return stock