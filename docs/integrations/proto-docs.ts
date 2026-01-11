/**
 * Astro integration that generates RPC reference documentation from protobuf files at build time.
 * The generated MDX file is placed in src/content/docs/reference/ so Starlight picks it up.
 */
import type { AstroIntegration } from "astro";
import protobuf from "protobufjs";
import * as fs from "fs";
import * as path from "path";

interface ProtoDocsOptions {
  /** Path to the proto directory, relative to project root */
  protoDir: string;
  /** Output path for the generated MDX file, relative to project root */
  outputPath: string;
}

interface FieldInfo {
  name: string;
  type: string;
  id: number;
  comment: string | null;
  optional: boolean;
  repeated: boolean;
  map: { keyType: string; valueType: string } | null;
}

interface MessageInfo {
  name: string;
  fullName: string;
  comment: string | null;
  fields: FieldInfo[];
  oneofs: { name: string; fields: string[]; comment: string | null }[];
  nestedMessages: MessageInfo[];
  nestedEnums: EnumInfo[];
}

interface EnumInfo {
  name: string;
  fullName: string;
  comment: string | null;
  values: { name: string; value: number; comment: string | null }[];
}

interface MethodInfo {
  name: string;
  comment: string | null;
  requestType: string;
  responseType: string;
  requestStream: boolean;
  responseStream: boolean;
}

interface ServiceInfo {
  name: string;
  fullName: string;
  comment: string | null;
  methods: MethodInfo[];
}

function getComment(obj: protobuf.ReflectionObject): string | null {
  return obj.comment || null;
}

function getFieldType(field: protobuf.Field): string {
  if (field instanceof protobuf.MapField) {
    return `map<${field.keyType}, ${field.type}>`;
  }
  return field.type;
}

function extractEnum(enumType: protobuf.Enum): EnumInfo {
  const values = Object.entries(enumType.values).map(([name, value]) => ({
    name,
    value,
    comment: enumType.comments?.[name] || null,
  }));

  return {
    name: enumType.name,
    fullName: enumType.fullName,
    comment: getComment(enumType),
    values,
  };
}

function extractMessage(msgType: protobuf.Type): MessageInfo {
  const fields: FieldInfo[] = [];
  const oneofs: { name: string; fields: string[]; comment: string | null }[] =
    [];

  // Extract oneofs first so we can skip their fields from the main list
  const oneofFieldNames = new Set<string>();
  if (msgType.oneofs) {
    for (const [oneofName, oneof] of Object.entries(msgType.oneofs)) {
      const fieldNames = oneof.fieldsArray.map((f) => f.name);
      fieldNames.forEach((n) => oneofFieldNames.add(n));
      oneofs.push({
        name: oneofName,
        fields: fieldNames,
        comment: getComment(oneof),
      });
    }
  }

  // Extract fields
  for (const field of msgType.fieldsArray) {
    fields.push({
      name: field.name,
      type: getFieldType(field),
      id: field.id,
      comment: field.comment || null,
      optional: field.optional || false,
      repeated: field.repeated,
      map:
        field instanceof protobuf.MapField
          ? { keyType: field.keyType, valueType: field.type }
          : null,
    });
  }

  // Extract nested messages
  const nestedMessages: MessageInfo[] = [];
  const nestedEnums: EnumInfo[] = [];

  for (const nested of msgType.nestedArray) {
    if (nested instanceof protobuf.Type) {
      nestedMessages.push(extractMessage(nested));
    } else if (nested instanceof protobuf.Enum) {
      nestedEnums.push(extractEnum(nested));
    }
  }

  return {
    name: msgType.name,
    fullName: msgType.fullName,
    comment: getComment(msgType),
    fields,
    oneofs,
    nestedMessages,
    nestedEnums,
  };
}

function extractService(service: protobuf.Service): ServiceInfo {
  const methods: MethodInfo[] = [];

  for (const method of service.methodsArray) {
    methods.push({
      name: method.name,
      comment: getComment(method),
      requestType: method.requestType,
      responseType: method.responseType,
      requestStream: method.requestStream || false,
      responseStream: method.responseStream || false,
    });
  }

  return {
    name: service.name,
    fullName: service.fullName,
    comment: getComment(service),
    methods,
  };
}

function escapeMarkdown(text: string): string {
  return text.replace(/[<>]/g, (c) => (c === "<" ? "&lt;" : "&gt;"));
}

function formatType(type: string): string {
  return `\`${escapeMarkdown(type)}\``;
}

function generateFieldTable(fields: FieldInfo[]): string {
  if (fields.length === 0) return "_No fields_\n";

  let table = "| Field | Type | ID | Description |\n";
  table += "|-------|------|-----|-------------|\n";

  for (const field of fields) {
    const typeStr = field.repeated
      ? `repeated ${field.type}`
      : field.optional
      ? `optional ${field.type}`
      : field.type;
    const desc = field.comment ? escapeMarkdown(field.comment) : "";
    table += `| \`${field.name}\` | ${formatType(typeStr)} | ${
      field.id
    } | ${desc} |\n`;
  }

  return table;
}

function generateEnumDoc(enumInfo: EnumInfo): string {
  let doc = `### \`${enumInfo.name}\`\n\n`;

  if (enumInfo.comment) {
    doc += `${enumInfo.comment}\n\n`;
  }

  doc += "| Value | Number | Description |\n";
  doc += "|-------|--------|-------------|\n";

  for (const val of enumInfo.values) {
    const desc = val.comment ? escapeMarkdown(val.comment) : "";
    doc += `| \`${val.name}\` | ${val.value} | ${desc} |\n`;
  }

  doc += "\n";
  return doc;
}

function generateMessageDoc(msg: MessageInfo, depth: number = 3): string {
  const heading = "#".repeat(depth);
  let doc = `${heading} \`${msg.name}\`\n\n`;

  if (msg.comment) {
    doc += `${msg.comment}\n\n`;
  }

  // Generate oneof sections
  if (msg.oneofs.length > 0) {
    for (const oneof of msg.oneofs) {
      const oneofFields = msg.fields.filter((f) =>
        oneof.fields.includes(f.name)
      );
      doc += `**Oneof \`${oneof.name}\`**: `;
      if (oneof.comment) {
        doc += `${oneof.comment}\n\n`;
      } else {
        doc += "One of the following:\n\n";
      }
      doc += generateFieldTable(oneofFields);
      doc += "\n";
    }

    // Show non-oneof fields
    const oneofFieldNames = new Set(msg.oneofs.flatMap((o) => o.fields));
    const regularFields = msg.fields.filter(
      (f) => !oneofFieldNames.has(f.name)
    );
    if (regularFields.length > 0) {
      doc += "**Fields:**\n\n";
      doc += generateFieldTable(regularFields);
      doc += "\n";
    }
  } else {
    doc += generateFieldTable(msg.fields);
    doc += "\n";
  }

  // Nested messages
  for (const nested of msg.nestedMessages) {
    doc += generateMessageDoc(nested, Math.min(depth + 1, 6));
  }

  // Nested enums
  for (const nested of msg.nestedEnums) {
    doc += generateEnumDoc(nested);
  }

  return doc;
}

function generateServiceDoc(
  service: ServiceInfo,
  allMessages: Map<string, MessageInfo>
): string {
  let doc = `## Service: \`${service.name}\`\n\n`;

  if (service.comment) {
    doc += `${service.comment}\n\n`;
  }

  for (const method of service.methods) {
    doc += `### \`${method.name}\`\n\n`;

    if (method.comment) {
      doc += `${method.comment}\n\n`;
    }

    const reqStream = method.requestStream ? "stream " : "";
    const resStream = method.responseStream ? "stream " : "";

    doc += "```protobuf\n";
    doc += `rpc ${method.name}(${reqStream}${method.requestType}) returns (${resStream}${method.responseType})\n`;
    doc += "```\n\n";

    // Link to request/response types
    doc += `**Request:** [\`${method.requestType}\`](#${method.requestType
      .toLowerCase()
      .replace(/\./g, "")})\n\n`;
    doc += `**Response:** [\`${method.responseType}\`](#${method.responseType
      .toLowerCase()
      .replace(/\./g, "")})\n\n`;
  }

  return doc;
}

function parseProtoFile(filePath: string): {
  services: ServiceInfo[];
  messages: MessageInfo[];
  enums: EnumInfo[];
} {
  // Read and parse with alternateCommentMode to capture inline and block comments
  const content = fs.readFileSync(filePath, "utf-8");
  const result = protobuf.parse(content, {
    keepCase: true,
    alternateCommentMode: true,
  });
  const root = result.root;

  const services: ServiceInfo[] = [];
  const messages: MessageInfo[] = [];
  const enums: EnumInfo[] = [];

  function traverse(namespace: protobuf.NamespaceBase) {
    for (const nested of namespace.nestedArray) {
      if (nested instanceof protobuf.Service) {
        services.push(extractService(nested));
      } else if (nested instanceof protobuf.Type) {
        messages.push(extractMessage(nested));
      } else if (nested instanceof protobuf.Enum) {
        enums.push(extractEnum(nested));
      } else if (nested instanceof protobuf.Namespace) {
        traverse(nested);
      }
    }
  }

  traverse(root);

  return { services, messages, enums };
}

function generateMdx(
  services: ServiceInfo[],
  messages: MessageInfo[],
  enums: EnumInfo[]
): string {
  const allMessages = new Map<string, MessageInfo>();
  for (const msg of messages) {
    allMessages.set(msg.name, msg);
  }

  let mdx = `---
title: RPC Reference
description: Complete reference for Silo's gRPC API, auto-generated from protobuf definitions.
---

{/* This file is auto-generated from proto/silo.proto. Do not edit directly. */}

This page documents Silo's gRPC API. The protobuf definitions are the source of truth for all RPC interfaces.

`;

  // Services first
  if (services.length > 0) {
    mdx += "## Services\n\n";
    for (const service of services) {
      mdx += generateServiceDoc(service, allMessages);
    }
  }

  // Then messages
  if (messages.length > 0) {
    mdx += "## Messages\n\n";
    for (const msg of messages) {
      mdx += generateMessageDoc(msg);
    }
  }

  // Then enums
  if (enums.length > 0) {
    mdx += "## Enums\n\n";
    for (const enumInfo of enums) {
      mdx += generateEnumDoc(enumInfo);
    }
  }

  return mdx;
}

export default function protoDocs(options: ProtoDocsOptions): AstroIntegration {
  return {
    name: "proto-docs",
    hooks: {
      "astro:config:setup": async ({ config, logger }) => {
        const projectRoot = config.root.pathname;
        const protoDir = path.join(projectRoot, options.protoDir);
        const outputPath = path.join(projectRoot, options.outputPath);

        logger.info(`Generating proto docs from ${protoDir}`);

        // Find all .proto files
        const protoFiles = fs
          .readdirSync(protoDir)
          .filter((f) => f.endsWith(".proto"))
          .map((f) => path.join(protoDir, f));

        // Only process silo.proto for now (gubernator is an external dependency)
        const siloProto = protoFiles.find((f) => f.endsWith("silo.proto"));
        if (!siloProto) {
          logger.warn("No silo.proto file found");
          return;
        }

        try {
          const { services, messages, enums } = parseProtoFile(siloProto);
          const mdx = generateMdx(services, messages, enums);

          // Ensure output directory exists
          const outputDir = path.dirname(outputPath);
          if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir, { recursive: true });
          }

          fs.writeFileSync(outputPath, mdx);
          logger.info(`Generated RPC reference at ${outputPath}`);
        } catch (err) {
          logger.error(`Failed to generate proto docs: ${err}`);
          throw err;
        }
      },
    },
  };
}
