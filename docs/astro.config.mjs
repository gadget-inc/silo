// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import protoDocs from './integrations/proto-docs.ts';

// https://astro.build/config
export default defineConfig({
	base: '/silo',
	integrations: [
		protoDocs({
			protoDir: '../proto',
			outputPath: 'src/content/docs/reference/rpc-reference.mdx',
		}),
		starlight({
			title: '',
			logo: {
				src: './src/assets/logo.svg',
				alt: 'Silo',
			},
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/gadget-inc/silo' }],
			sidebar: [
				{
					label: 'Introduction',
					slug: 'introduction',
				},
				{
					label: 'Guides',
					items: [
						{ slug: 'guides/enqueueing' },
						{ slug: 'guides/running-workers' },
						{ slug: 'guides/cancel-restart-delete' },
						{ slug: 'guides/concurrency-limits' },
						{ slug: 'guides/querying' },
						{ slug: 'guides/deployment' },
						{ slug: 'guides/observability' },
						{ slug: 'guides/silo-vs-temporal' },
						{ slug: 'guides/internals' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ slug: 'reference/server-configuration' },
						{ slug: 'reference/rpc-reference' },
						{ slug: 'reference/siloctl' },
					],
				},
			],
		}),
	],
});
