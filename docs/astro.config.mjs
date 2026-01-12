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
			title: 'Silo',
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/gadget-inc/silo' }],
			sidebar: [
				{
					label: 'Introduction',
					slug: 'introduction',
				},
				{
					label: 'Guides',
					autogenerate: { directory: 'guides' },
				},
				{
					label: 'Reference',
					autogenerate: { directory: 'reference' },
				},
			],
		}),
	],
});
