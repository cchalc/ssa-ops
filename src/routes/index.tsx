import { Container, Flex, Heading, Text } from "@radix-ui/themes";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/")({
	component: HomePage,
});

function HomePage() {
	return (
		<Container size="2" py="9">
			<Flex direction="column" gap="5" align="center">
				<Heading size="8">Welcome</Heading>
				<Text size="4" color="gray">
					Your app is ready. Try switching font themes!
				</Text>
			</Flex>
		</Container>
	);
}
