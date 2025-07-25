import {
  Button,
  ButtonGroup,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  List,
  ListItem,
  ListItemText,
  Paper,
  Stack,
  TextField,
  Tooltip,
  useTheme,
} from '@mui/material';
import React from 'react';
import { MaterialSymbol } from './nodes/icons';
import { useTemplates } from './templates-provider';
import type { SectionTemplate } from './types/api';

export interface EditTemplatesDialogProps {
  open: boolean;
  onClose: () => void;
}

interface RenamingState {
  target: string;
  selectAll?: boolean;
  scrollTo?: boolean;
}

function EditTemplatesDialog({ open, onClose }: EditTemplatesDialogProps) {
  const theme = useTheme();
  const [templates, setTemplates] = useTemplates();
  const templateKeys = Object.keys(templates);
  const [renaming, setRenaming] = React.useState<RenamingState | null>(null);
  const [newId, setNewId] = React.useState('');
  const renamingRef = React.useRef<HTMLInputElement>(null);

  React.useEffect(() => {
    if (!renamingRef.current || !renaming) {
      return;
    }

    renamingRef.current.focus();
    if (renaming.selectAll) {
      renamingRef.current.select();
    }
    if (renaming.scrollTo) {
      renamingRef.current.scrollTo({ behavior: 'smooth' });
    }
  }, [renaming]);

  const handleSubmitRenaming = () => {
    const newTemplates: Record<string, SectionTemplate> = {};
    // rebuild the templates in a way that keeps ordering
    for (const [id, template] of Object.entries(templates)) {
      if (id === renaming?.target) {
        newTemplates[newId] = template;
      } else {
        newTemplates[id] = template;
      }
    }
    setRenaming(null);
    setNewId('');
    setTemplates(newTemplates);
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      fullWidth
      maxWidth="sm"
      keepMounted={false}
    >
      <DialogTitle>Templates</DialogTitle>
      <DialogContent>
        <Paper>
          <List disablePadding sx={{ maxHeight: '24rem', overflow: 'auto' }}>
            {templateKeys.length > 0 ? (
              templateKeys.map((id) => (
                <ListItem key={id} divider>
                  <Stack
                    direction="row"
                    alignItems="center"
                    width="100%"
                    height="3em"
                  >
                    {renaming?.target === id ? (
                      <ListItemText>
                        <form onSubmit={handleSubmitRenaming}>
                          <TextField
                            size="small"
                            fullWidth
                            value={newId}
                            onChange={(ev) => {
                              setNewId(ev.target.value);
                            }}
                            inputRef={renamingRef}
                            onSubmit={handleSubmitRenaming}
                          />
                        </form>
                      </ListItemText>
                    ) : (
                      <ListItemText>{id}</ListItemText>
                    )}
                    <ButtonGroup variant="contained">
                      {renaming?.target === id ? (
                        <Button onClick={handleSubmitRenaming}>
                          <MaterialSymbol symbol="check" />
                        </Button>
                      ) : (
                        <Tooltip title="Rename">
                          <Button
                            onClick={() => {
                              setRenaming({ target: id });
                              setNewId(id);
                            }}
                          >
                            <MaterialSymbol symbol="text_select_start" />
                          </Button>
                        </Tooltip>
                      )}
                      <Tooltip title="Edit">
                        <Button>
                          <MaterialSymbol symbol="edit" />
                        </Button>
                      </Tooltip>
                      {/* MUI has a 1px alignment error when displaying buttons of different color side by side in a ButtonGroup.
                      Using a different variant hides the error and also puts less focus on the "Delete" button. */}
                      <Tooltip title="Delete">
                        <Button
                          variant="outlined"
                          color="error"
                          onClick={() =>
                            setTemplates((prev) => {
                              const newTemplates = { ...prev };
                              delete newTemplates[id];
                              return newTemplates;
                            })
                          }
                        >
                          <MaterialSymbol symbol="delete" />
                        </Button>
                      </Tooltip>
                    </ButtonGroup>
                  </Stack>
                </ListItem>
              ))
            ) : (
              <ListItem divider>
                <ListItemText
                  slotProps={{
                    primary: { color: theme.palette.text.disabled },
                  }}
                >
                  No templates available
                </ListItemText>
              </ListItem>
            )}
          </List>
          <Divider />
          <ListItem>
            <Stack justifyContent="center" width="100%">
              <Button
                onClick={() => {
                  const baseId = 'new_template';
                  let newId = baseId;
                  let i = 0;
                  while (newId in templates) {
                    newId = `${baseId}_${++i}`;
                  }
                  setNewId(newId);

                  setTemplates((prev) => {
                    return {
                      ...prev,
                      [newId]: {
                        inputs: {},
                        outputs: [],
                        buffers: {},
                        ops: {},
                      },
                    };
                  });
                  setRenaming({
                    target: newId,
                    selectAll: true,
                    scrollTo: true,
                  });
                }}
              >
                <MaterialSymbol symbol="add" />
              </Button>
            </Stack>
          </ListItem>
        </Paper>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
}

export default EditTemplatesDialog;
