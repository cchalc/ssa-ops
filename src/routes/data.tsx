import { createFileRoute } from '@tanstack/react-router'
import { Container, Flex, Heading, Text, Table, Badge, Code, Box } from '@radix-ui/themes'
import { useLiveQuery } from '@tanstack/react-db'
import { testItemsCollection, type TestItem } from '../db'

export const Route = createFileRoute('/data')({
  component: DataPage,
})

function DataPage() {
  const { data, isLoading, isError } = useLiveQuery(
    (q) =>
      q
        .from({ testItems: testItemsCollection })
        .orderBy(({ testItems }) => testItems.id, 'asc'),
    []
  )

  // Cast to proper type (Electric collection doesn't carry types through)
  const items = data as unknown as TestItem[] | undefined

  return (
    <Container size="3" py="6">
      <Flex direction="column" gap="4">
        <Flex justify="between" align="center">
          <Heading size="7">Data Explorer</Heading>
          <Badge color={isError ? 'red' : isLoading ? 'yellow' : 'green'} size="2">
            {isError ? 'Error' : isLoading ? 'Syncing...' : 'Live'}
          </Badge>
        </Flex>

        <Text color="gray" size="2">
          Synced from Lakebase via Electric SQL • {items?.length ?? 0} items
        </Text>

        <Table.Root>
          <Table.Header>
            <Table.Row>
              <Table.ColumnHeaderCell>ID</Table.ColumnHeaderCell>
              <Table.ColumnHeaderCell>Name</Table.ColumnHeaderCell>
              <Table.ColumnHeaderCell>Description</Table.ColumnHeaderCell>
              <Table.ColumnHeaderCell>Price</Table.ColumnHeaderCell>
              <Table.ColumnHeaderCell>Quantity</Table.ColumnHeaderCell>
              <Table.ColumnHeaderCell>Created</Table.ColumnHeaderCell>
            </Table.Row>
          </Table.Header>

          <Table.Body>
            {items?.map((item) => (
              <Table.Row key={item.id}>
                <Table.Cell>
                  <Code>{item.id}</Code>
                </Table.Cell>
                <Table.Cell>{item.name}</Table.Cell>
                <Table.Cell>
                  <Text color="gray">{item.description || '—'}</Text>
                </Table.Cell>
                <Table.Cell>
                  {item.price !== null ? `$${Number(item.price).toFixed(2)}` : '—'}
                </Table.Cell>
                <Table.Cell>{item.quantity ?? '—'}</Table.Cell>
                <Table.Cell>
                  <Text size="1" color="gray">
                    {item.created_at instanceof Date
                      ? item.created_at.toLocaleDateString()
                      : '—'}
                  </Text>
                </Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>

        {(!items || items.length === 0) && !isLoading && (
          <Box py="6">
            <Text color="gray" align="center">
              No items found. Make sure Electric SQL is running.
            </Text>
          </Box>
        )}
      </Flex>
    </Container>
  )
}
