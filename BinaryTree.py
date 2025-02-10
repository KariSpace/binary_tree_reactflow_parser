class TreeNode:
    def __init__(self, node_id, label, node_type=None):
        self.id = node_id
        self.label = label
        self.type = node_type
        self.left = None
        self.right = None

class BinaryTree:
    def __init__(self, nodes, edges):
        self.nodes = {node['id']: TreeNode(node['id'], node['data']['label'], node.get('type')) for node in nodes}
        self.root = self.nodes['1']
        self.build_tree(edges)

    def build_tree(self, edges):
        for edge in edges:
            source_id = edge['source']
            target_id = edge['target']
            label = edge.get('label')
            source_node = self.nodes[source_id]
            target_node = self.nodes[target_id]

            if label == 'True' or not source_node.left:
                source_node.left = target_node
            else:
                source_node.right = target_node

    def print_tree(self, node=None, level=0, side="root"):
        if node is not None:
            print(" " * 4 * level + f"{side}: {node.label} ({node.id})")
            self.print_tree(node.left, level + 1, "left")
            self.print_tree(node.right, level + 1, "right")

    def generate_if_else(self, order_data, node=None, depth=0):
        lists_dict = order_data.get("line_items")
        if node is None:
            node = self.root

        code = ""
        indent = "    " * depth

        if node.type == "condition":
            code += f"{indent}if {node.label}:\n"
            code += self.generate_if_else(node.left, depth + 1)
            code += f"{indent}else:\n"
            code += self.generate_if_else(node.right, depth + 1)
        elif node.type == "fulfill":
            code += f"{indent}print('{node.label}')\n"
        elif node.type == "split":
            list_name = node.label
            if list_name:
                items_list = lists_dict.get(list_name)
                if items_list:
                    code += f"{indent}if any(item in {items_list} for item in order.get('items')):\n"
                    code += self.generate_if_else(node.left, depth + 1)
                    code += f"{indent}else:\n"
                    code += self.generate_if_else(node.right, depth + 1)
                else:
                    print(f"List '{list_name}' not found in the provided dictionary.")
            else:
                print("List name not specified in the node data.")

        return code
