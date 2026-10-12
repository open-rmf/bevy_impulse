import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Drawer,
  IconButton,
  InputAdornment,
  List,
  ListItem,
  ListItemText,
  Paper,
  Stack,
  Tab,
  Tabs,
  TextField,
  Tooltip,
  Typography,
  useTheme,
} from '@mui/material';
import React from 'react';
import { v4 as uuidv4 } from 'uuid';
import { useDiagramProperties } from './diagram-properties-provider';
import {
  type DiagramSidePanelTab,
  useDiagramSidePanel,
} from './diagram-side-panel-controller';
import {
  ScriptEnvironmentWorkspace,
  type ScriptNodeEnvironmentBinding,
} from './forms/script-environment-workspace';
import { useLoadContext } from './load-context-provider';
import { MaterialSymbol } from './nodes';
import { RunPanel } from './run-button';
import type { InputExample } from './types/api';
import { useDiagramSidePanelWidth } from './use-responsive-edit-popover-position';

const EmptyInputExample: InputExample = {
  description: '',
  value: undefined,
};

export interface DiagramSidePanelProps {
  runRequestJson: string;
  onRunRequestJsonChange: (requestJson: string) => void;
  scriptNodeBinding?: ScriptNodeEnvironmentBinding;
}

function getInputExampleRequestJson(input: InputExample): string {
  if (typeof input.value === 'string') {
    return input.value;
  }

  return JSON.stringify(input.value, null, 2) || '';
}

function DiagramSidePanel({
  runRequestJson,
  onRunRequestJsonChange,
  scriptNodeBinding,
}: DiagramSidePanelProps) {
  const {
    state: { open, tab, expanded },
    close,
    showTab,
    setWidth,
  } = useDiagramSidePanel();
  const [diagramProperties, setDiagramProperties] = useDiagramProperties();
  const loadContext = useLoadContext();
  const theme = useTheme();
  const drawerWidth = useDiagramSidePanelWidth();
  const [resizing, setResizing] = React.useState(false);
  const widthTransition = theme.transitions.create('width', {
    easing: theme.transitions.easing.easeInOut,
    duration: resizing ? 0 : theme.transitions.duration.standard,
  });
  const [copyTooltipText, setCopyTooltipText] = React.useState(
    'Copy this input example into clipboard',
  );
  const [openAddExampleDialog, setOpenAddExampleDialog] = React.useState(false);
  const [newInputExample, setNewInputExample] =
    React.useState<InputExample>(EmptyInputExample);
  const [localExamples, setLocalExamples] = React.useState<
    (InputExample & { id: string })[]
  >([]);

  React.useEffect(() => {
    const examples = loadContext?.diagram.input_examples ?? [];
    setLocalExamples(examples.map((ex) => ({ ...ex, id: uuidv4() })));
    setDiagramProperties({
      description: loadContext?.diagram.description ?? '',
      input_examples: examples,
      script_environments: loadContext?.diagram.script_environments ?? {},
    });
  }, [loadContext, setDiagramProperties]);

  const inputExampleInvalid = React.useMemo(() => {
    return (
      newInputExample.description === '' ||
      newInputExample.value === undefined ||
      newInputExample.value === ''
    );
  }, [newInputExample]);

  return (
    <>
      <Drawer
        sx={{
          width: drawerWidth,
          flexShrink: 0,
          transition: widthTransition,
          '& .MuiDrawer-paper': {
            width: drawerWidth,
            display: 'flex',
            flexDirection: 'column',
            overflowX: 'hidden',
            transition: widthTransition,
          },
        }}
        variant="persistent"
        anchor="right"
        open={open}
        data-testid="diagram-side-panel-paper"
        data-expanded={expanded}
        style={{ width: drawerWidth, transition: widthTransition }}
        slotProps={{
          paper: {
            style: {
              width: drawerWidth,
              transition: widthTransition,
            },
          },
        }}
      >
        <Box
          onPointerDown={(event) => {
            if (event.button !== 0) return;
            event.preventDefault();
            event.currentTarget.setPointerCapture(event.pointerId);
            setResizing(true);
          }}
          onPointerMove={(event) => {
            if (resizing) setWidth(window.innerWidth - event.clientX);
          }}
          onLostPointerCapture={() => setResizing(false)}
          sx={{
            position: 'absolute',
            left: 0,
            top: 0,
            bottom: 0,
            width: 6,
            zIndex: 1,
            cursor: 'col-resize',
            touchAction: 'none',
          }}
        />
        <Stack
          direction="row"
          sx={{
            alignItems: 'center',
            px: 2,
            pt: 1,
          }}
        >
          <Tabs
            value={tab}
            onChange={(_, value: DiagramSidePanelTab) => showTab(value)}
            sx={{
              minHeight: 40,
              minWidth: 0,
              flex: 1,
              '& .MuiTab-root': {
                minWidth: 0,
                minHeight: 40,
                px: 1,
                fontSize: '0.75rem',
              },
            }}
          >
            <Tab
              value="properties"
              label="Properties"
              icon={<MaterialSymbol symbol="info" />}
              iconPosition="start"
            />
            <Tab
              value="run"
              label="Run"
              icon={<MaterialSymbol symbol="play_arrow" />}
              iconPosition="start"
            />
            <Tab
              value="environments"
              label="Environments"
              icon={<MaterialSymbol symbol="code" />}
              iconPosition="start"
            />
          </Tabs>
          <Tooltip title="Hide this panel">
            <IconButton onClick={close} sx={{ ml: 'auto', flexShrink: 0 }}>
              <MaterialSymbol symbol="close" />
            </IconButton>
          </Tooltip>
        </Stack>
        <Divider />
        <Box
          sx={{
            display: tab === 'run' ? 'flex' : 'none',
            flexDirection: 'column',
            flex: 1,
            minHeight: 0,
          }}
        >
          <RunPanel
            requestJsonString={runRequestJson}
            onRequestJsonStringChange={onRunRequestJsonChange}
          />
        </Box>
        <Box
          sx={{
            display: tab === 'properties' ? 'flex' : 'none',
            flexDirection: 'column',
            flex: 1,
            minHeight: 0,
          }}
        >
          <Stack spacing={2} sx={{ minHeight: 0, overflowY: 'auto', p: 2 }}>
            <Stack direction="row">
              <Stack direction="row" spacing={2} sx={{ alignItems: 'center' }}>
                <Typography variant="h6">Description</Typography>
                <Tooltip title="General description of what this diagram achieves, as well as any other relevant information that may help a user understand its execution.">
                  <MaterialSymbol symbol="info" fontSize="large" />
                </Tooltip>
              </Stack>
            </Stack>
            <TextField
              fullWidth
              multiline
              rows={10}
              variant="outlined"
              value={diagramProperties?.description ?? ''}
              slotProps={{
                htmlInput: { sx: { fontFamily: 'monospace' } },
              }}
              onChange={(d) =>
                setDiagramProperties((prev) => ({
                  ...prev,
                  description: d.target.value,
                }))
              }
              sx={{ backgroundColor: theme.palette.background.paper }}
            />
            <Divider />
            <Stack direction="row">
              <Stack direction="row" spacing={2} sx={{ alignItems: 'center' }}>
                <Typography variant="h6">Input Examples</Typography>
                <Tooltip title="Input examples that can be executed with this workflow">
                  <MaterialSymbol symbol="info" fontSize="large" />
                </Tooltip>
              </Stack>
              <Stack direction="row" sx={{ marginLeft: 'auto' }}>
                <Tooltip title="Add input example">
                  <IconButton onClick={() => setOpenAddExampleDialog(true)}>
                    <MaterialSymbol symbol="add" />
                  </IconButton>
                </Tooltip>
              </Stack>
            </Stack>
            <Paper>
              <List>
                {localExamples.length > 0 ? (
                  localExamples.map((input, index) => (
                    <ListItem key={input.id}>
                      <TextField
                        fullWidth
                        multiline
                        variant="outlined"
                        value={getInputExampleRequestJson(input)}
                        rows="6"
                        slotProps={{
                          htmlInput: {
                            sx: { fontFamily: 'monospace' },
                          },
                          input: {
                            endAdornment: (
                              <InputAdornment position="end">
                                <Stack direction="column">
                                  <Tooltip title={input.description}>
                                    <IconButton>
                                      <MaterialSymbol
                                        symbol="info"
                                        fontSize="large"
                                      />
                                    </IconButton>
                                  </Tooltip>
                                  <Tooltip title="Delete input example">
                                    <IconButton
                                      onClick={() => {
                                        setLocalExamples((prev) => [
                                          ...prev.slice(0, index),
                                          ...prev.slice(index + 1),
                                        ]);
                                        setDiagramProperties((prev) => {
                                          const prevInputExamples =
                                            prev.input_examples ?? [];
                                          if (
                                            index >= prevInputExamples.length
                                          ) {
                                            return prev;
                                          }
                                          return {
                                            ...prev,
                                            input_examples: [
                                              ...prevInputExamples.slice(
                                                0,
                                                index,
                                              ),
                                              ...prevInputExamples.slice(
                                                index + 1,
                                              ),
                                            ],
                                          };
                                        });
                                      }}
                                    >
                                      <MaterialSymbol
                                        symbol="delete"
                                        fontSize="large"
                                      />
                                    </IconButton>
                                  </Tooltip>
                                  <Tooltip title={copyTooltipText}>
                                    <IconButton
                                      onClick={async () => {
                                        const content =
                                          getInputExampleRequestJson(input);
                                        await navigator.clipboard.writeText(
                                          content,
                                        );
                                        setCopyTooltipText(
                                          'Copied into clipboard!',
                                        );
                                        setTimeout(() => {
                                          setCopyTooltipText(
                                            'Copy this input example',
                                          );
                                        }, 3000);
                                      }}
                                    >
                                      <MaterialSymbol
                                        symbol="content_copy"
                                        fontSize="large"
                                      />
                                    </IconButton>
                                  </Tooltip>
                                  <Tooltip title="Run diagram with this input example">
                                    <IconButton
                                      onClick={() => {
                                        onRunRequestJsonChange(
                                          getInputExampleRequestJson(input),
                                        );
                                        showTab('run');
                                      }}
                                    >
                                      <MaterialSymbol
                                        symbol="play_arrow"
                                        fontSize="large"
                                      />
                                    </IconButton>
                                  </Tooltip>
                                </Stack>
                              </InputAdornment>
                            ),
                          },
                        }}
                      />
                    </ListItem>
                  ))
                ) : (
                  <ListItem sx={{ textAlign: 'center' }}>
                    <ListItemText
                      slotProps={{
                        primary: { color: theme.palette.text.disabled },
                      }}
                      primary="No input examples available"
                    />
                  </ListItem>
                )}
              </List>
            </Paper>
          </Stack>
        </Box>
        <Box
          sx={{
            display: tab === 'environments' ? 'flex' : 'none',
            flexDirection: 'column',
            flex: 1,
            minHeight: 0,
          }}
        >
          <ScriptEnvironmentWorkspace scriptNodeBinding={scriptNodeBinding} />
        </Box>
      </Drawer>
      <Dialog
        onClose={() => {
          setOpenAddExampleDialog(false);
          setNewInputExample(EmptyInputExample);
        }}
        open={openAddExampleDialog}
        fullWidth
        maxWidth="sm"
        keepMounted={false}
      >
        <DialogTitle>Add input example</DialogTitle>
        <Divider />
        <DialogContent>
          <Stack spacing={2}>
            <Typography variant="body1">Value</Typography>
            <TextField
              fullWidth
              multiline
              rows={6}
              variant="outlined"
              value={newInputExample.value}
              onChange={(e) =>
                setNewInputExample((prev) => ({
                  ...prev,
                  value: e.target.value as string,
                }))
              }
            />
            <Typography variant="body1">Description</Typography>
            <TextField
              fullWidth
              multiline
              rows={3}
              variant="outlined"
              value={newInputExample.description}
              onChange={(e) =>
                setNewInputExample((prev) => ({
                  ...prev,
                  description: e.target.value,
                }))
              }
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button
            variant="contained"
            onClick={() => {
              const newLocalExample = { ...newInputExample, id: uuidv4() };
              setLocalExamples((prev) => [...prev, newLocalExample]);
              setDiagramProperties((prev) => {
                const prevInputExamples = prev.input_examples ?? [];
                return {
                  ...prev,
                  input_examples: [...prevInputExamples, newInputExample],
                };
              });
              setOpenAddExampleDialog(false);
              setNewInputExample(EmptyInputExample);
            }}
            startIcon={<MaterialSymbol symbol="add" />}
            disabled={inputExampleInvalid}
          >
            Add
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
}

export default DiagramSidePanel;
