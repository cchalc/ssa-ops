import { Link } from '@tanstack/react-router'
import { Flex, Text } from '@radix-ui/themes'
import { ThemePicker } from './ThemePicker'

export function Header() {
  return (
    <header style={{ borderBottom: '1px solid var(--gray-6)' }}>
      <Flex align="center" justify="between" py="3" px="4">
        <Link to="/" style={{ textDecoration: 'none' }}>
          <Text size="5" weight="bold">
            SSA-Ops
          </Text>
        </Link>
        <Flex gap="4" align="center">
          <Link to="/lakebase" style={{ textDecoration: 'none' }}>
            <Text size="2" color="gray">Lakebase</Text>
          </Link>
          <Link to="/data" style={{ textDecoration: 'none' }}>
            <Text size="2" color="gray">Data (Electric)</Text>
          </Link>
          <ThemePicker />
        </Flex>
      </Flex>
    </header>
  )
}
