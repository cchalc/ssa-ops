import {
	Badge,
	Box,
	Button,
	Card,
	Code,
	Container,
	Dialog,
	Flex,
	Heading,
	IconButton,
	Table,
	Text,
	TextField,
} from "@radix-ui/themes";
import { createFileRoute } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { Database, Plus, RefreshCw, Trash2 } from "lucide-react";
import { useId, useState } from "react";
import {
	addTestItem,
	deleteTestItem,
	fetchStats,
	fetchTestItems,
	type LakebaseStats,
	type TestItem,
} from "../lib/lakebase";

// Server functions for Lakebase operations
const getItems = createServerFn().handler(async () => {
	return fetchTestItems();
});

const getStats = createServerFn().handler(async () => {
	return fetchStats();
});

const createItem = createServerFn().handler(
	async (ctx: {
		data: {
			name: string;
			description?: string;
			price?: number;
			quantity?: number;
		};
	}) => {
		return addTestItem(ctx.data);
	},
);

const removeItem = createServerFn().handler(
	async (ctx: { data: { id: number } }) => {
		return deleteTestItem(ctx.data.id);
	},
);

export const Route = createFileRoute("/lakebase")({
	component: LakebasePage,
	loader: async () => {
		const [items, stats] = await Promise.all([getItems(), getStats()]);
		return { items, stats };
	},
});

function LakebasePage() {
	const { items: initialItems, stats: initialStats } = Route.useLoaderData();
	const [items, setItems] = useState<TestItem[]>(initialItems);
	const [stats, setStats] = useState<LakebaseStats>(initialStats);
	const [isRefreshing, setIsRefreshing] = useState(false);
	const [isAddDialogOpen, setIsAddDialogOpen] = useState(false);
	const formId = useId();
	const nameId = `${formId}-name`;
	const descriptionId = `${formId}-description`;
	const priceId = `${formId}-price`;
	const quantityId = `${formId}-quantity`;

	const handleRefresh = async () => {
		setIsRefreshing(true);
		try {
			const [newItems, newStats] = await Promise.all([getItems(), getStats()]);
			setItems(newItems);
			setStats(newStats);
		} finally {
			setIsRefreshing(false);
		}
	};

	const handleAddItem = async (formData: FormData) => {
		const name = formData.get("name") as string;
		const description = formData.get("description") as string;
		const price = formData.get("price") as string;
		const quantity = formData.get("quantity") as string;

		await createItem({
			data: {
				name,
				description: description || undefined,
				price: price ? Number.parseFloat(price) : undefined,
				quantity: quantity ? Number.parseInt(quantity, 10) : undefined,
			},
		});
		setIsAddDialogOpen(false);
		handleRefresh();
	};

	const handleDelete = async (id: number) => {
		await removeItem({ data: { id } });
		handleRefresh();
	};

	return (
		<Container size="3" py="6">
			<Flex direction="column" gap="5">
				{/* Header */}
				<Flex justify="between" align="center">
					<Flex align="center" gap="3">
						<Database size={28} />
						<Heading size="7">Lakebase Explorer</Heading>
					</Flex>
					<Flex gap="2">
						<Badge color="green" size="2" variant="soft">
							Direct Connection
						</Badge>
						<Button
							variant="soft"
							onClick={handleRefresh}
							disabled={isRefreshing}
						>
							<RefreshCw
								size={16}
								className={isRefreshing ? "animate-spin" : ""}
							/>
							Refresh
						</Button>
					</Flex>
				</Flex>

				{/* Connection Info */}
				<Text color="gray" size="2">
					Connected to <Code>ssa-ops-dev</Code> • Database:{" "}
					<Code>ssa_ops_dev</Code> • PostgreSQL 17
				</Text>

				{/* Stats Cards */}
				<Flex gap="4">
					<Card style={{ flex: 1 }}>
						<Flex direction="column" gap="1">
							<Text size="2" color="gray">
								Total Items
							</Text>
							<Text size="6" weight="bold">
								{stats.totalItems}
							</Text>
						</Flex>
					</Card>
					<Card style={{ flex: 1 }}>
						<Flex direction="column" gap="1">
							<Text size="2" color="gray">
								Total Inventory Value
							</Text>
							<Text size="6" weight="bold">
								${stats.totalValue.toFixed(2)}
							</Text>
						</Flex>
					</Card>
					<Card style={{ flex: 1 }}>
						<Flex direction="column" gap="1">
							<Text size="2" color="gray">
								Total Quantity
							</Text>
							<Text size="6" weight="bold">
								{stats.totalQuantity}
							</Text>
						</Flex>
					</Card>
					<Card style={{ flex: 1 }}>
						<Flex direction="column" gap="1">
							<Text size="2" color="gray">
								Last Updated
							</Text>
							<Text size="6" weight="bold">
								{stats.lastUpdated
									? new Date(stats.lastUpdated).toLocaleTimeString()
									: "—"}
							</Text>
						</Flex>
					</Card>
				</Flex>

				{/* Actions */}
				<Flex justify="between" align="center">
					<Heading size="4">Test Items</Heading>
					<Dialog.Root open={isAddDialogOpen} onOpenChange={setIsAddDialogOpen}>
						<Dialog.Trigger>
							<Button>
								<Plus size={16} />
								Add Item
							</Button>
						</Dialog.Trigger>
						<Dialog.Content maxWidth="450px">
							<Dialog.Title>Add New Item</Dialog.Title>
							<Dialog.Description size="2" color="gray">
								Create a new test item in Lakebase
							</Dialog.Description>
							<form
								onSubmit={(e) => {
									e.preventDefault();
									handleAddItem(new FormData(e.currentTarget));
								}}
							>
								<Flex direction="column" gap="3" mt="4">
									<Flex direction="column" gap="1">
										<Text as="label" htmlFor={nameId} size="2" weight="medium">
											Name *
										</Text>
										<TextField.Root
											id={nameId}
											name="name"
											placeholder="Item name"
											required
										/>
									</Flex>
									<Flex direction="column" gap="1">
										<Text
											as="label"
											htmlFor={descriptionId}
											size="2"
											weight="medium"
										>
											Description
										</Text>
										<TextField.Root
											id={descriptionId}
											name="description"
											placeholder="Description"
										/>
									</Flex>
									<Flex gap="3">
										<Flex direction="column" gap="1" style={{ flex: 1 }}>
											<Text
												as="label"
												htmlFor={priceId}
												size="2"
												weight="medium"
											>
												Price
											</Text>
											<TextField.Root
												id={priceId}
												name="price"
												type="number"
												step="0.01"
												placeholder="0.00"
											/>
										</Flex>
										<Flex direction="column" gap="1" style={{ flex: 1 }}>
											<Text
												as="label"
												htmlFor={quantityId}
												size="2"
												weight="medium"
											>
												Quantity
											</Text>
											<TextField.Root
												id={quantityId}
												name="quantity"
												type="number"
												placeholder="0"
											/>
										</Flex>
									</Flex>
									<Flex gap="3" justify="end" mt="2">
										<Dialog.Close>
											<Button variant="soft" color="gray">
												Cancel
											</Button>
										</Dialog.Close>
										<Button type="submit">Create Item</Button>
									</Flex>
								</Flex>
							</form>
						</Dialog.Content>
					</Dialog.Root>
				</Flex>

				{/* Data Table */}
				<Table.Root>
					<Table.Header>
						<Table.Row>
							<Table.ColumnHeaderCell>ID</Table.ColumnHeaderCell>
							<Table.ColumnHeaderCell>Name</Table.ColumnHeaderCell>
							<Table.ColumnHeaderCell>Description</Table.ColumnHeaderCell>
							<Table.ColumnHeaderCell align="right">
								Price
							</Table.ColumnHeaderCell>
							<Table.ColumnHeaderCell align="right">
								Quantity
							</Table.ColumnHeaderCell>
							<Table.ColumnHeaderCell>Created</Table.ColumnHeaderCell>
							<Table.ColumnHeaderCell>Updated</Table.ColumnHeaderCell>
							<Table.ColumnHeaderCell width="60px"></Table.ColumnHeaderCell>
						</Table.Row>
					</Table.Header>

					<Table.Body>
						{items.map((item) => (
							<Table.Row key={item.id}>
								<Table.Cell>
									<Code variant="ghost">{item.id}</Code>
								</Table.Cell>
								<Table.Cell>
									<Text weight="medium">{item.name}</Text>
								</Table.Cell>
								<Table.Cell>
									<Text color="gray">{item.description || "—"}</Text>
								</Table.Cell>
								<Table.Cell align="right">
									{item.price !== null ? (
										<Code color="green">${Number(item.price).toFixed(2)}</Code>
									) : (
										"—"
									)}
								</Table.Cell>
								<Table.Cell align="right">
									<Badge variant="soft">{item.quantity ?? 0}</Badge>
								</Table.Cell>
								<Table.Cell>
									<Text size="1" color="gray">
										{new Date(item.created_at).toLocaleDateString()}
									</Text>
								</Table.Cell>
								<Table.Cell>
									<Text size="1" color="gray">
										{new Date(item.updated_at).toLocaleTimeString()}
									</Text>
								</Table.Cell>
								<Table.Cell>
									<IconButton
										size="1"
										variant="ghost"
										color="red"
										onClick={() => handleDelete(item.id)}
									>
										<Trash2 size={14} />
									</IconButton>
								</Table.Cell>
							</Table.Row>
						))}
					</Table.Body>
				</Table.Root>

				{items.length === 0 && (
					<Box py="6">
						<Text color="gray" align="center">
							No items found. Click "Add Item" to create one.
						</Text>
					</Box>
				)}

				{/* Footer */}
				<Flex justify="center" pt="4">
					<Text size="1" color="gray">
						Data fetched directly from Lakebase Autoscaling via server functions
					</Text>
				</Flex>
			</Flex>
		</Container>
	);
}
