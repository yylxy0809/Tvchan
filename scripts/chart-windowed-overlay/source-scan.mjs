import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const typescriptPath = process.argv[2];
if (!typescriptPath) throw new Error("TypeScript compiler path is required");
const ts = require(typescriptPath);

let input = "";
for await (const chunk of process.stdin) input += chunk;
const request = JSON.parse(input);
const findings = [];
const uncertainties = [];
const findingKeys = new Set();
const uncertaintyKeys = new Set();

const MAX_AST_NODES = 20000;
const MAX_ALIASES = 512;
const MAX_ALIAS_PASSES = 8;
const MAX_EXPRESSION_DEPTH = 16;
const MAX_REACHABLE_DECLARATIONS = 512;

const fixtureManagerMethods = new Set([
  "subscribeChanSnapshots", "subscribeRealtimeBars", "getBars", "getBarsInternal",
  "loadBars", "getChanOverlay", "loadChan", "handleChanSnapshotMessage",
  "handleRealtimeBarUpdate",
]);
const semanticAreas = [
  {
    area: "bars-history-datafeed",
    path: "apps/web/src/tradingview/datafeed.ts",
    roots: ["function:createDatafeed"],
  },
  {
    area: "bars-history-manager",
    path: "apps/web/src/api/chartDataManager.ts",
    roots: ["method:ChartDataManager.getBars"],
  },
  {
    area: "overlay-subscription-dispatch",
    path: "apps/web/src/api/chartDataManager.ts",
    roots: [
      "method:ChartDataManager.subscribeChanOverlay",
      "method:ChartWebSocketClient.handleMessage",
    ],
  },
  {
    area: "workspace-overlay-lifecycle",
    path: "apps/web/src/components/ChartWorkspace.tsx",
    roots: ["function:ChartWorkspace"],
  },
  {
    area: "overlay-manager-request-resync-realtime",
    path: "apps/web/src/api/chanOverlayManager.ts",
    roots: [
      "method:ChanOverlayManager.request",
      "method:ChanOverlayManager.fetchFresh",
      "method:ChanOverlayManager.applyRealtime",
    ],
  },
  {
    area: "realtime-overlay-bridge",
    path: "apps/web/src/api/chanRealtimeOverlayBridge.ts",
    roots: [
      "method:ChanRealtimeOverlayBridge.hydrateHttp",
      "method:ChanRealtimeOverlayBridge.apply",
    ],
  },
];
const directBundleCalls = new Set(["getChartBundle", "getChartWindow", "getChartBundleHttp"]);
const networkCalls = new Set(["fetch", "getJson", "request", "apiUrl", "get"]);
const transportKinds = new Set(["get_chart_bundle", "subscribe_chart_bundle"]);
const endpointPattern = /\/api\/v[23]\/chart\/bundle(?:\b|[/?#])/i;

function normalizedPath(path) {
  return path.replaceAll("\\", "/");
}

function nodeName(node) {
  if (!node) return "";
  if (ts.isIdentifier(node) || ts.isPrivateIdentifier(node)) return node.text;
  if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node) || ts.isNumericLiteral(node)) return node.text;
  if (ts.isPropertyAccessExpression(node)) return node.name.text;
  return "";
}

function lineOf(sourceFile, node) {
  return sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile)).line + 1;
}

function addFinding(path, scope, sourceFile, node, token) {
  const line = lineOf(sourceFile, node);
  const key = `${path}\0${scope}\0${node.getStart(sourceFile)}\0${token}`;
  if (findingKeys.has(key)) return;
  findingKeys.add(key);
  findings.push({ path, scope, line, token });
}

function addUncertainty(path, scope, reason) {
  const key = `${path}\0${scope}\0${reason}`;
  if (uncertaintyKeys.has(key)) return;
  uncertaintyKeys.add(key);
  uncertainties.push({ path, scope, reason });
}

function unwrapExpression(node) {
  let current = node;
  while (current && (
    ts.isParenthesizedExpression(current) || ts.isAsExpression(current) ||
    ts.isTypeAssertionExpression(current) || ts.isNonNullExpression(current) ||
    (ts.isSatisfiesExpression && ts.isSatisfiesExpression(current))
  )) current = current.expression;
  return current;
}

function isBundleEndpoint(value) {
  return endpointPattern.test(value);
}

function looksBundleRelevant(node, sourceFile) {
  const text = node.getText(sourceFile).toLowerCase();
  return text.includes("bundle") && (text.includes("chart") || text.includes("api"));
}

function canRepresentEndpoint(node) {
  const expression = unwrapExpression(node);
  return ts.isIdentifier(expression) || ts.isStringLiteral(expression) ||
    ts.isNoSubstitutionTemplateLiteral(expression) || ts.isTemplateExpression(expression) ||
    ts.isBinaryExpression(expression) || ts.isPropertyAccessExpression(expression) ||
    ts.isElementAccessExpression(expression) || ts.isConditionalExpression(expression);
}

function sameResolution(left, right) {
  return left?.kind === right?.kind && left?.value === right?.value &&
    left?.reason === right?.reason && left?.relevant === right?.relevant;
}

function setResolution(map, key, value) {
  if (!key || !value || sameResolution(map.get(key), value)) return false;
  map.set(key, value);
  return true;
}

function createProgram(sources) {
  const sourceMap = new Map();
  for (const [path, content] of Object.entries(sources)) sourceMap.set(normalizedPath(path), String(content));
  const options = {
    allowJs: false,
    jsx: ts.JsxEmit.Preserve,
    noLib: true,
    noResolve: true,
    target: ts.ScriptTarget.Latest,
  };
  const host = ts.createCompilerHost(options, true);
  host.fileExists = (fileName) => sourceMap.has(normalizedPath(fileName));
  host.readFile = (fileName) => sourceMap.get(normalizedPath(fileName));
  host.getSourceFile = (fileName, languageVersion) => {
    const path = normalizedPath(fileName);
    const content = sourceMap.get(path);
    if (content === undefined) return undefined;
    const kind = path.toLowerCase().endsWith(".tsx") ? ts.ScriptKind.TSX : ts.ScriptKind.TS;
    return ts.createSourceFile(path, content, languageVersion, true, kind);
  };
  host.getCurrentDirectory = () => "/";
  host.getCanonicalFileName = normalizedPath;
  host.getNewLine = () => "\n";
  host.useCaseSensitiveFileNames = () => true;
  host.writeFile = () => {};
  return ts.createProgram([...sourceMap.keys()], options, host);
}

const program = createProgram(request.sources ?? {});
const checker = program.getTypeChecker();
const callableDeclarations = new Map();
const declarationIdentities = new Map();
const callableAliasTargets = new Map();
const callableAliasProblems = new Map();
const callableAliasDeclarations = [];

function declarationSymbol(node) {
  if (ts.isMethodDeclaration(node) || ts.isFunctionDeclaration(node)) {
    return node.name ? checker.getSymbolAtLocation(node.name) : undefined;
  }
  if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name)) {
    return checker.getSymbolAtLocation(node.name);
  }
  return undefined;
}

function declarationIdentity(node) {
  if (ts.isMethodDeclaration(node)) {
    const owner = node.parent && (ts.isClassDeclaration(node.parent) || ts.isClassExpression(node.parent))
      ? nodeName(node.parent.name)
      : "";
    return owner && nodeName(node.name) ? `method:${owner}.${nodeName(node.name)}` : "";
  }
  if (ts.isFunctionDeclaration(node) && node.name) return `function:${node.name.text}`;
  return "";
}

function collectCallableDeclarations(sourceFile) {
  const identities = new Map();
  declarationIdentities.set(normalizedPath(sourceFile.fileName), identities);
  function visit(node) {
    const isCallableVariable = ts.isVariableDeclaration(node) && node.initializer &&
      (ts.isArrowFunction(unwrapExpression(node.initializer)) || ts.isFunctionExpression(unwrapExpression(node.initializer)));
    if (ts.isMethodDeclaration(node) || ts.isFunctionDeclaration(node) || isCallableVariable) {
      const symbol = declarationSymbol(node);
      if (symbol) {
        const declarations = callableDeclarations.get(symbol) ?? [];
        if (!declarations.includes(node)) declarations.push(node);
        callableDeclarations.set(symbol, declarations);
      }
      const identity = declarationIdentity(node);
      if (identity) {
        const declarations = identities.get(identity) ?? [];
        declarations.push(node);
        identities.set(identity, declarations);
      }
    }
    if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name) && node.initializer) {
      callableAliasDeclarations.push(node);
    }
    ts.forEachChild(node, visit);
  }
  visit(sourceFile);
}

for (const sourceFile of program.getSourceFiles()) collectCallableDeclarations(sourceFile);

function referencedCallableSymbol(expression) {
  const node = unwrapExpression(expression);
  if (!node) return undefined;
  if (ts.isIdentifier(node)) return checker.getSymbolAtLocation(node);
  if (ts.isPropertyAccessExpression(node)) return checker.getSymbolAtLocation(node.name);
  if (ts.isElementAccessExpression(node) && node.argumentExpression) {
    const property = unwrapExpression(node.argumentExpression);
    if (ts.isStringLiteral(property) || ts.isNoSubstitutionTemplateLiteral(property)) {
      return checker.getSymbolAtLocation(node.argumentExpression) ?? checker.getSymbolAtLocation(node);
    }
  }
  return undefined;
}

function invocationTarget(expression) {
  const node = unwrapExpression(expression);
  if (ts.isCallExpression(node) && ts.isPropertyAccessExpression(unwrapExpression(node.expression))) {
    const callee = unwrapExpression(node.expression);
    if (["call", "apply", "bind"].includes(callee.name.text)) return { node: unwrapExpression(callee.expression), helper: callee.name.text };
  }
  if (ts.isPropertyAccessExpression(node) && ["call", "apply", "bind"].includes(node.name.text)) {
    return { node: unwrapExpression(node.expression), helper: node.name.text };
  }
  return { node, helper: "" };
}

function resolveReachableDeclaration(expression) {
  const invocation = invocationTarget(expression);
  if (invocation.helper && ts.isElementAccessExpression(invocation.node)) {
    const property = unwrapExpression(invocation.node.argumentExpression);
    if (!ts.isStringLiteral(property) && !ts.isNoSubstitutionTemplateLiteral(property)) {
      return { problem: `unsupported dynamic computed ${invocation.helper} receiver` };
    }
  }
  const symbol = referencedCallableSymbol(invocation.node);
  if (!symbol) return {};
  const declarations = callableDeclarations.get(symbol) ?? [];
  if (declarations.length > 1) return { problem: `ambiguous callable symbol has ${declarations.length} declarations` };
  if (declarations.length === 1) return { target: declarations[0] };
  if (callableAliasProblems.has(symbol)) return { problem: callableAliasProblems.get(symbol) };
  const aliasTarget = callableAliasTargets.get(symbol);
  return aliasTarget ? { target: aliasTarget } : {};
}

for (let pass = 0; pass < MAX_ALIAS_PASSES; pass += 1) {
  let changed = false;
  for (const declaration of callableAliasDeclarations) {
    const alias = declarationSymbol(declaration);
    const resolution = resolveReachableDeclaration(declaration.initializer);
    if (alias && resolution.problem) callableAliasProblems.set(alias, resolution.problem);
    if (alias && resolution.target && callableAliasTargets.get(alias) !== resolution.target) {
      callableAliasTargets.set(alias, resolution.target);
      changed = true;
    }
  }
  if (!changed) break;
  if (pass === MAX_ALIAS_PASSES - 1) {
    addUncertainty("<scanner>", "reachability", `callable alias graph did not converge within ${MAX_ALIAS_PASSES} passes`);
  }
}

function inspectScope(path, scope, sourceFile, root, onReachableDeclaration = null) {
  const stringAliases = new Map();
  const callableAliases = new Map();
  const declarations = [];
  let nodeCount = 0;
  let exceeded = false;

  function symbolKey(node) {
    if (!node || !ts.isIdentifier(node)) return null;
    return checker.getSymbolAtLocation(node) ?? `name:${node.text}`;
  }

  function callableFromName(name) {
    if (directBundleCalls.has(name)) return { kind: "forbidden", value: name };
    if (networkCalls.has(name)) return { kind: "network", value: name };
    return null;
  }

  function touchesCallAlias(expression) {
    const root = unwrapExpression(expression);
    if (ts.isArrowFunction(root) || ts.isFunctionExpression(root)) return false;
    let touched = false;
    function visit(node) {
      if (touched || ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) return;
      if (node !== root && (ts.isArrowFunction(node) || ts.isFunctionExpression(node))) return;
      if (ts.isIdentifier(node) && callableFromName(node.text)) {
        touched = true;
        return;
      }
      if (ts.isPropertyAccessExpression(node) && callableFromName(node.name.text)) {
        touched = true;
        return;
      }
      if (ts.isElementAccessExpression(node) && /api|client|http|request|transport|loader/i.test(node.expression.getText(sourceFile))) {
        touched = true;
        return;
      }
      ts.forEachChild(node, visit);
    }
    visit(root);
    return touched;
  }

  function resolveString(expression, depth = 0, seen = new Set()) {
    if (!expression || depth > MAX_EXPRESSION_DEPTH) {
      return { kind: "dynamic", relevant: true, reason: "string alias expression depth limit exceeded" };
    }
    const node = unwrapExpression(expression);
    if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
      return { kind: "static", value: node.text };
    }
    if (ts.isIdentifier(node)) {
      const key = symbolKey(node);
      if (seen.has(key)) return { kind: "dynamic", relevant: false, reason: "cyclic string alias" };
      const known = stringAliases.get(key);
      if (known) return known;
      return { kind: "dynamic", relevant: false, reason: `unresolved string identifier ${node.text}` };
    }
    if (ts.isBinaryExpression(node) && node.operatorToken.kind === ts.SyntaxKind.PlusToken) {
      const left = resolveString(node.left, depth + 1, new Set(seen));
      const right = resolveString(node.right, depth + 1, new Set(seen));
      if (left.kind === "static" && right.kind === "static") return { kind: "static", value: left.value + right.value };
      return {
        kind: "dynamic",
        relevant: Boolean(left.relevant || right.relevant || looksBundleRelevant(node, sourceFile)),
        reason: "dynamic string concatenation touching a possible bundle endpoint",
      };
    }
    if (ts.isTemplateExpression(node)) {
      let value = node.head.text;
      let relevant = looksBundleRelevant(node, sourceFile);
      for (const span of node.templateSpans) {
        const part = resolveString(span.expression, depth + 1, new Set(seen));
        if (part.kind !== "static") {
          return { kind: "dynamic", relevant: Boolean(relevant || part.relevant), reason: "dynamic template touching a possible bundle endpoint" };
        }
        value += part.value + span.literal.text;
      }
      return { kind: "static", value };
    }
    return {
      kind: "dynamic",
      relevant: looksBundleRelevant(node, sourceFile),
      reason: "unsupported dynamic endpoint expression",
    };
  }

  function resolveCallable(expression, depth = 0) {
    if (!expression || depth > MAX_EXPRESSION_DEPTH) return { kind: "dynamic", reason: "call alias expression depth limit exceeded" };
    const node = unwrapExpression(expression);
    const invocation = invocationTarget(node);
    if (invocation.helper) {
      const target = resolveCallable(invocation.node, depth + 1);
      if (target) return target;
      if (ts.isElementAccessExpression(invocation.node)) {
        return { kind: "dynamic", reason: `unsupported dynamic computed ${invocation.helper} receiver` };
      }
      return null;
    }
    if (ts.isIdentifier(node)) return callableAliases.get(symbolKey(node)) ?? callableFromName(node.text);
    if (ts.isPropertyAccessExpression(node)) return callableFromName(node.name.text);
    if (ts.isElementAccessExpression(node)) {
      const property = resolveString(node.argumentExpression, depth + 1);
      if (property.kind === "static") return callableFromName(property.value);
      const receiver = node.expression.getText(sourceFile);
      if (/api|client|http|request|transport|loader/i.test(receiver)) {
        return { kind: "dynamic", reason: `unsupported computed call alias on ${receiver}` };
      }
      return null;
    }
    if (ts.isConditionalExpression(node)) {
      const whenTrue = resolveCallable(node.whenTrue, depth + 1);
      const whenFalse = resolveCallable(node.whenFalse, depth + 1);
      if (sameResolution(whenTrue, whenFalse)) return whenTrue;
      if (whenTrue || whenFalse) return { kind: "dynamic", reason: "unsupported conditional call alias" };
    }
    if (touchesCallAlias(node)) return { kind: "dynamic", reason: "unsupported expression touching a call alias" };
    return null;
  }

  function registerBinding(name, propertyName) {
    if (ts.isIdentifier(name)) {
      const resolution = callableFromName(propertyName || name.text);
      if (resolution) setResolution(callableAliases, symbolKey(name), resolution);
      return;
    }
    if (ts.isObjectBindingPattern(name)) {
      for (const element of name.elements) {
        const property = nodeName(element.propertyName) || (ts.isIdentifier(element.name) ? element.name.text : "");
        registerBinding(element.name, property);
      }
    }
  }

  function collect(node) {
    nodeCount += 1;
    if (nodeCount > MAX_AST_NODES) {
      exceeded = true;
      return;
    }
    if (ts.isParameter(node)) registerBinding(node.name, "");
    if (ts.isVariableDeclaration(node) && ts.isIdentifier(node.name) && node.initializer) {
      const list = node.parent;
      if (ts.isVariableDeclarationList(list) && (list.flags & (ts.NodeFlags.Const | ts.NodeFlags.Let)) !== 0) {
        if (declarations.length < MAX_ALIASES) declarations.push(node);
        else exceeded = true;
      }
    }
    ts.forEachChild(node, collect);
  }
  collect(root);
  if (exceeded) {
    addUncertainty(path, scope, `AST/alias analysis limit exceeded (${MAX_AST_NODES} nodes or ${MAX_ALIASES} aliases)`);
    return;
  }

  let changedOnLastPass = false;
  for (let pass = 0; pass < MAX_ALIAS_PASSES; pass += 1) {
    let changed = false;
    for (const declaration of declarations) {
      const key = symbolKey(declaration.name);
      const stringValue = resolveString(declaration.initializer);
      if (stringValue.kind === "static" || stringValue.relevant) changed = setResolution(stringAliases, key, stringValue) || changed;
      const callable = resolveCallable(declaration.initializer);
      if (callable) changed = setResolution(callableAliases, key, callable) || changed;
    }
    changedOnLastPass = changed;
    if (!changed) break;
  }
  if (changedOnLastPass) addUncertainty(path, scope, `alias propagation did not converge within ${MAX_ALIAS_PASSES} passes`);

  function visit(node) {
    if (ts.isCallExpression(node)) {
      if (onReachableDeclaration) {
        const called = resolveReachableDeclaration(node.expression);
        if (called.target) onReachableDeclaration(called.target);
        if (called.problem) addUncertainty(path, scope, `${called.problem} at line ${lineOf(sourceFile, node)}`);
        for (const argument of node.arguments) {
          const callback = resolveReachableDeclaration(argument);
          if (callback.target) onReachableDeclaration(callback.target);
          if (callback.problem) addUncertainty(path, scope, `${callback.problem} at line ${lineOf(sourceFile, argument)}`);
        }
      }
      const callable = resolveCallable(node.expression);
      if (callable?.kind === "forbidden") addFinding(path, scope, sourceFile, node.expression, callable.value);
      if (callable?.kind === "dynamic") addUncertainty(path, scope, `${callable.reason} at line ${lineOf(sourceFile, node)}`);
      if (callable?.kind === "network") {
        for (const argument of node.arguments) {
          if (!canRepresentEndpoint(argument)) continue;
          const endpoint = resolveString(argument);
          if (endpoint.kind === "static" && isBundleEndpoint(endpoint.value)) {
            addFinding(path, scope, sourceFile, argument, "bundle endpoint alias");
          } else if (endpoint.kind === "dynamic" && endpoint.relevant) {
            addUncertainty(path, scope, `${endpoint.reason} at line ${lineOf(sourceFile, argument)}`);
          }
        }
      }
    }
    if (ts.isPropertyAccessExpression(node) && node.name.text === "bundle") {
      addFinding(path, scope, sourceFile, node, ".bundle");
    }
    if (ts.isElementAccessExpression(node)) {
      const property = resolveString(node.argumentExpression);
      if (property.kind === "static" && property.value === "bundle") {
        addFinding(path, scope, sourceFile, node, "computed bundle property");
      }
    }
    if (ts.isPropertyAssignment(node) && nodeName(node.name) === "type") {
      const value = resolveString(node.initializer);
      if (value.kind === "static" && transportKinds.has(value.value)) {
        addFinding(path, scope, sourceFile, node, "bundle transport command literal");
      }
    }
    ts.forEachChild(node, visit);
  }
  visit(root);
}

const sourceFilesByPath = new Map();
for (const sourceFile of program.getSourceFiles()) {
  const path = normalizedPath(sourceFile.fileName);
  sourceFilesByPath.set(path, sourceFile);
  if (path.endsWith("/client.ts") || path === "client.ts") continue;
  for (const diagnostic of program.getSyntacticDiagnostics(sourceFile)) {
    const position = diagnostic.start ?? 0;
    const line = sourceFile.getLineAndCharacterOfPosition(position).line + 1;
    addUncertainty(path, "parse", `TypeScript syntax error at line ${line}: ${ts.flattenDiagnosticMessageText(diagnostic.messageText, " ")}`);
  }

}

function sourceFileForConfiguredPath(configuredPath) {
  const normalized = normalizedPath(configuredPath);
  for (const [path, sourceFile] of sourceFilesByPath) {
    if (path === normalized || path.endsWith(`/${normalized}`)) return sourceFile;
  }
  return undefined;
}

function inspectSemanticArea(area) {
  const sourceFile = sourceFileForConfiguredPath(area.path);
  if (!sourceFile) {
    addUncertainty(area.path, area.area, "required semantic source file not found in TypeScript program");
    return;
  }
  const path = normalizedPath(sourceFile.fileName);
  const identities = declarationIdentities.get(path) ?? new Map();
  const queue = [];
  for (const identity of area.roots) {
    const roots = identities.get(identity) ?? [];
    if (roots.length === 1) queue.push({ node: roots[0], identity });
    else if (roots.length === 0) addUncertainty(path, area.area, `required semantic entrypoint ${identity} not found`);
    else addUncertainty(path, area.area, `required semantic entrypoint ${identity} is ambiguous (${roots.length} declarations)`);
  }
  const visited = new Set();
  while (queue.length) {
    const current = queue.shift();
    if (visited.has(current.node)) continue;
    if (visited.size >= MAX_REACHABLE_DECLARATIONS) {
      addUncertainty(path, area.area, `reachable declaration limit exceeded (${MAX_REACHABLE_DECLARATIONS})`);
      return;
    }
    visited.add(current.node);
    const owner = current.node.getSourceFile();
    inspectScope(
      normalizedPath(owner.fileName),
      `${area.area}:${current.identity}`,
      owner,
      current.node,
      (target) => queue.push({ node: target, identity: declarationIdentity(target) || `callable@${lineOf(target.getSourceFile(), target)}` }),
    );
  }
}

if (request.requireProductionScopes) {
  for (const area of semanticAreas) inspectSemanticArea(area);
} else {
  for (const sourceFile of program.getSourceFiles()) {
    const path = normalizedPath(sourceFile.fileName);
    if (path.endsWith("/client.ts") || path === "client.ts") continue;
    if (path.endsWith("/chartDataManager.ts") || path === "chartDataManager.ts") {
    const methods = new Map();
    function collectMethods(node) {
      if (ts.isMethodDeclaration(node)) {
        const name = nodeName(node.name);
          if (fixtureManagerMethods.has(name)) methods.set(name, node);
      }
      ts.forEachChild(node, collectMethods);
    }
    collectMethods(sourceFile);
    for (const [name, method] of methods) inspectScope(path, name, sourceFile, method);
    } else {
      inspectScope(path, "production-entrypoint", sourceFile, sourceFile);
    }
  }
}

process.stdout.write(JSON.stringify({ findings, uncertainties, scanner: "typescript-checker-semantic-roots-v3" }));
